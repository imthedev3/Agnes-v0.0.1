from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from dataclasses import dataclass
from enum import Enum
import aiohttp
import aiomysql
from elasticsearch import AsyncElasticsearch
from collections import defaultdict
import pytz

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

class AggregationType(Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    DISTINCT = "distinct"
    PERCENTILE = "percentile"

@dataclass
class MetricDefinition:
    name: str
    type: MetricType
    description: str
    labels: List[str]
    unit: Optional[str] = None
    aggregations: List[AggregationType] = None

@dataclass
class TimeSeriesPoint:
    timestamp: datetime
    value: float
    labels: Dict[str, str]

class AnalyticsEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics: Dict[str, MetricDefinition] = {}
        self.storage = TimeSeriesStorage(config['storage'])
        self.logger = logging.getLogger(__name__)
    
    def register_metric(self, metric: MetricDefinition):
        """Register new metric"""
        self.metrics[metric.name] = metric
    
    async def record_metric(self,
                          name: str,
                          value: float,
                          labels: Dict[str, str] = None,
                          timestamp: Optional[datetime] = None):
        """Record metric value"""
        if name not in self.metrics:
            raise ValueError(f"Metric {name} not registered")
        
        metric = self.metrics[name]
        if labels:
            # Validate labels
            unknown_labels = set(labels.keys()) - set(metric.labels)
            if unknown_labels:
                raise ValueError(
                    f"Unknown labels for metric {name}: {unknown_labels}"
                )
        
        point = TimeSeriesPoint(
            timestamp=timestamp or datetime.utcnow(),
            value=value,
            labels=labels or {}
        )
        
        await self.storage.store_point(name, point)
    
    async def query_metric(self,
                          name: str,
                          start_time: datetime,
                          end_time: datetime,
                          aggregation: AggregationType,
                          interval: str = "1h",
                          labels: Dict[str, str] = None) -> pd.DataFrame:
        """Query metric data"""
        if name not in self.metrics:
            raise ValueError(f"Metric {name} not registered")
        
        metric = self.metrics[name]
        if aggregation not in metric.aggregations:
            raise ValueError(
                f"Aggregation {aggregation} not supported for metric {name}"
            )
        
        points = await self.storage.query_points(
            name,
            start_time,
            end_time,
            labels
        )
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'timestamp': p.timestamp,
                'value': p.value,
                **p.labels
            }
            for p in points
        ])
        
        if df.empty:
            return df
        
        # Resample and aggregate
        df.set_index('timestamp', inplace=True)
        agg_func = self._get_aggregation_func(aggregation)
        
        return df.resample(interval)['value'].agg(agg_func)
    
    def _get_aggregation_func(self,
                             aggregation: AggregationType) -> Callable:
        """Get aggregation function"""
        if aggregation == AggregationType.SUM:
            return np.sum
        elif aggregation == AggregationType.AVG:
            return np.mean
        elif aggregation == AggregationType.MIN:
            return np.min
        elif aggregation == AggregationType.MAX:
            return np.max
        elif aggregation == AggregationType.COUNT:
            return len
        elif aggregation == AggregationType.DISTINCT:
            return lambda x: len(set(x))
        elif aggregation == AggregationType.PERCENTILE:
            return lambda x: np.percentile(x, 95)
        else:
            raise ValueError(f"Unknown aggregation type: {aggregation}")

class TimeSeriesStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.client = None
        self._setup_client()
    
    def _setup_client(self):
        """Setup storage client"""
        if self.type == 'elasticsearch':
            self.client = AsyncElasticsearch([self.config['url']])
        elif self.type == 'mysql':
            self.pool = aiomysql.create_pool(**self.config['connection'])
    
    async def store_point(self, metric: str, point: TimeSeriesPoint):
        """Store time series point"""
        if self.type == 'elasticsearch':
            await self.client.index(
                index=f"metrics-{metric}",
                body={
                    'timestamp': point.timestamp.isoformat(),
                    'value': point.value,
                    'labels': point.labels
                }
            )
        elif self.type == 'mysql':
            pool = await self.pool
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO metrics
                        (metric_name, timestamp, value, labels)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            metric,
                            point.timestamp,
                            point.value,
                            json.dumps(point.labels)
                        )
                    )
                await conn.commit()
    
    async def query_points(self,
                          metric: str,
                          start_time: datetime,
                          end_time: datetime,
                          labels: Optional[Dict[str, str]] = None) -> List[TimeSeriesPoint]:
        """Query time series points"""
        if self.type == 'elasticsearch':
            query = {
                'bool': {
                    'must': [
                        {
                            'range': {
                                'timestamp': {
                                    'gte': start_time.isoformat(),
                                    'lte': end_time.isoformat()
                                }
                            }
                        }
                    ]
                }
            }
            
            if labels:
                for key, value in labels.items():
                    query['bool']['must'].append({
                        'term': {
                            f'labels.{key}': value
                        }
                    })
            
            result = await self.client.search(
                index=f"metrics-{metric}",
                body={'query': query}
            )
            
            return [
                TimeSeriesPoint(
                    timestamp=datetime.fromisoformat(hit['_source']['timestamp']),
                    value=hit['_source']['value'],
                    labels=hit['_source']['labels']
                )
                for hit in result['hits']['hits']
            ]
        
        elif self.type == 'mysql':
            pool = await self.pool
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT timestamp, value, labels
                    FROM metrics
                    WHERE metric_name = %s
                    AND timestamp BETWEEN %s AND %s
                    """
                    params = [metric, start_time, end_time]
                    
                    if labels:
                        query += " AND labels @> %s"
                        params.append(json.dumps(labels))
                    
                    await cur.execute(query, params)
                    rows = await cur.fetchall()
                    
                    return [
                        TimeSeriesPoint(
                            timestamp=row[0],
                            value=row[1],
                            labels=json.loads(row[2])
                        )
                        for row in rows
                    ]

class AnalyticsProcessor:
    def __init__(self, engine: AnalyticsEngine):
        self.engine = engine
    
    async def calculate_trends(self,
                             metric: str,
                             window: str = "7d",
                             aggregation: AggregationType = AggregationType.AVG) -> Dict[str, float]:
        """Calculate metric trends"""
        end_time = datetime.utcnow()
        start_time = end_time - pd.Timedelta(window)
        
        df = await self.engine.query_metric(
            metric,
            start_time,
            end_time,
            aggregation,
            interval="1d"
        )
        
        if df.empty:
            return {}
        
        # Calculate various trend indicators
        current = df.iloc[-1]
        previous = df.iloc[-2] if len(df) > 1 else None
        avg = df.mean()
        
        return {
            'current': current,
            'previous': previous,
            'change': ((current - previous) / previous * 100) 
                if previous else 0,
            'average': avg,
            'min': df.min(),
            'max': df.max()
        }
    
    async def detect_anomalies(self,
                             metric: str,
                             threshold: float = 2.0) -> List[Dict[str, Any]]:
        """Detect metric anomalies"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=1)
        
        df = await self.engine.query_metric(
            metric,
            start_time,
            end_time,
            AggregationType.AVG,
            interval="5m"
        )
        
        if df.empty:
            return []
        
        # Calculate rolling statistics
        rolling_mean = df.rolling(window=12).mean()
        rolling_std = df.rolling(window=12).std()
        
        # Detect anomalies
        anomalies = []
        for timestamp, value in df.items():
            if value > rolling_mean[timestamp] + threshold * rolling_std[timestamp]:
                anomalies.append({
                    'timestamp': timestamp,
                    'value': value,
                    'expected': rolling_mean[timestamp],
                    'deviation': (
                        value - rolling_mean[timestamp]
                    ) / rolling_std[timestamp]
                })
        
        return anomalies

class ReportGenerator:
    def __init__(self, engine: AnalyticsEngine):
        self.engine = engine
        self.processor = AnalyticsProcessor(engine)
    
    async def generate_report(self,
                            metrics: List[str],
                            start_time: datetime,
                            end_time: datetime) -> Dict[str, Any]:
        """Generate analytics report"""
        report = {
            'period': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'metrics': {}
        }
        
        for metric in metrics:
            # Get metric data
            data = await self.engine.query_metric(
                metric,
                start_time,
                end_time,
                AggregationType.AVG,
                interval="1h"
            )
            
            # Calculate trends
            trends = await self.processor.calculate_trends(metric)
            
            # Detect anomalies
            anomalies = await self.processor.detect_anomalies(metric)
            
            report['metrics'][metric] = {
                'data': data.to_dict(),
                'trends': trends,
                'anomalies': anomalies,
                'summary': {
                    'total': len(data),
                    'average': data.mean(),
                    'min': data.min(),
                    'max': data.max()
                }
            }
        
        return report

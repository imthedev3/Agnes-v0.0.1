from typing import Dict, Any, List, Optional, Union
import asyncio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import jinja2
import pdfkit
import json
import aiofiles
import logging
from dataclasses import dataclass
from enum import Enum
import boto3
import aiomysql
from elasticsearch import AsyncElasticsearch
import plotly.graph_objects as go
import plotly.express as px
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle

class ReportType(Enum):
    METRICS = "metrics"
    AUDIT = "audit"
    USAGE = "usage"
    PERFORMANCE = "performance"
    SECURITY = "security"
    COMPLIANCE = "compliance"
    FINANCIAL = "financial"

class ReportFormat(Enum):
    PDF = "pdf"
    EXCEL = "excel"
    CSV = "csv"
    HTML = "html"
    JSON = "json"

@dataclass
class Report:
    id: str
    type: ReportType
    name: str
    format: ReportFormat
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime
    generated_at: Optional[datetime] = None
    status: str = "pending"
    error: Optional[str] = None

class ReportGenerator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates/reports')
        )
        self.storage = ReportStorage(config['storage'])
        self.logger = logging.getLogger(__name__)
        
        # Set plotting style
        plt.style.use('seaborn')
        sns.set_palette("husl")
    
    async def generate(self, report: Report):
        """Generate report"""
        try:
            # Collect data
            data = await self._collect_data(report)
            
            # Process data
            processed_data = await self._process_data(data, report)
            
            # Generate visualizations
            visuals = await self._generate_visuals(processed_data, report)
            
            # Generate report in requested format
            if report.format == ReportFormat.PDF:
                output = await self._generate_pdf(processed_data, visuals, report)
            elif report.format == ReportFormat.EXCEL:
                output = await self._generate_excel(processed_data, report)
            elif report.format == ReportFormat.CSV:
                output = await self._generate_csv(processed_data, report)
            elif report.format == ReportFormat.HTML:
                output = await self._generate_html(processed_data, visuals, report)
            elif report.format == ReportFormat.JSON:
                output = await self._generate_json(processed_data, report)
            
            # Store report
            await self.storage.store(report, output)
            
            report.status = "completed"
            report.generated_at = datetime.utcnow()
        
        except Exception as e:
            report.status = "failed"
            report.error = str(e)
            self.logger.error(f"Failed to generate report: {e}")
            raise
    
    async def _collect_data(self,
                          report: Report) -> Dict[str, Any]:
        """Collect report data"""
        if report.type == ReportType.METRICS:
            return await self._collect_metrics_data(report)
        elif report.type == ReportType.AUDIT:
            return await self._collect_audit_data(report)
        elif report.type == ReportType.USAGE:
            return await self._collect_usage_data(report)
        elif report.type == ReportType.PERFORMANCE:
            return await self._collect_performance_data(report)
        elif report.type == ReportType.SECURITY:
            return await self._collect_security_data(report)
        elif report.type == ReportType.COMPLIANCE:
            return await self._collect_compliance_data(report)
        elif report.type == ReportType.FINANCIAL:
            return await self._collect_financial_data(report)
    
    async def _process_data(self,
                          data: Dict[str, Any],
                          report: Report) -> Dict[str, Any]:
        """Process report data"""
        # Convert to pandas DataFrame
        dfs = {}
        for key, value in data.items():
            if isinstance(value, list):
                dfs[key] = pd.DataFrame(value)
            elif isinstance(value, dict):
                dfs[key] = pd.DataFrame.from_dict(
                    value,
                    orient='index'
                )
        
        # Apply transformations
        for key, df in dfs.items():
            # Time series processing
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                df = df.resample('1H').mean()
            
            # Fill missing values
            df.fillna(method='ffill', inplace=True)
            
            # Calculate statistics
            if report.type == ReportType.METRICS:
                df['rolling_mean'] = df.rolling(window=24).mean()
                df['rolling_std'] = df.rolling(window=24).std()
            
            dfs[key] = df
        
        return dfs
    
    async def _generate_visuals(self,
                              data: Dict[str, pd.DataFrame],
                              report: Report) -> Dict[str, str]:
        """Generate report visualizations"""
        visuals = {}
        
        for key, df in data.items():
            # Time series plot
            if isinstance(df.index, pd.DatetimeIndex):
                fig = go.Figure()
                
                for column in df.columns:
                    fig.add_trace(go.Scatter(
                        x=df.index,
                        y=df[column],
                        name=column
                    ))
                
                fig.update_layout(
                    title=f"{key} Over Time",
                    xaxis_title="Time",
                    yaxis_title="Value"
                )
                
                visuals[f"{key}_timeseries"] = fig
            
            # Distribution plot
            if df.shape[1] > 0:
                fig = px.box(
                    df,
                    title=f"{key} Distribution"
                )
                visuals[f"{key}_distribution"] = fig
            
            # Correlation heatmap
            if df.shape[1] > 1:
                fig = px.imshow(
                    df.corr(),
                    title=f"{key} Correlation"
                )
                visuals[f"{key}_correlation"] = fig
        
        return visuals
    
    async def _generate_pdf(self,
                          data: Dict[str, pd.DataFrame],
                          visuals: Dict[str, Any],
                          report: Report) -> bytes:
        """Generate PDF report"""
        # Create PDF document
        doc = SimpleDocTemplate(
            "report.pdf",
            pagesize=letter
        )
        
        elements = []
        
        # Add title
        elements.append(Paragraph(
            report.name,
            self.styles['Title']
        ))
        
        # Add summary table
        summary_data = []
        for key, df in data.items():
            summary_data.append([
                key,
                df.shape[0],
                df.shape[1],
                df.memory_usage().sum() / 1024
            ])
        
        summary_table = Table([
            ['Dataset', 'Rows', 'Columns', 'Size (KB)']
        ] + summary_data)
        
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(summary_table)
        
        # Add visualizations
        for name, fig in visuals.items():
            img_path = f"{name}.png"
            fig.write_image(img_path)
            elements.append(Image(img_path))
        
        # Build PDF
        doc.build(elements)
        
        with open("report.pdf", "rb") as f:
            return f.read()
    
    async def _generate_excel(self,
                            data: Dict[str, pd.DataFrame],
                            report: Report) -> bytes:
        """Generate Excel report"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output) as writer:
            for key, df in data.items():
                df.to_excel(
                    writer,
                    sheet_name=key,
                    index=True
                )
        
        return output.getvalue()
    
    async def _generate_csv(self,
                          data: Dict[str, pd.DataFrame],
                          report: Report) -> bytes:
        """Generate CSV report"""
        output = io.BytesIO()
        
        for key, df in data.items():
            output.write(f"# {key}\n".encode())
            output.write(df.to_csv().encode())
            output.write(b"\n")
        
        return output.getvalue()
    
    async def _generate_html(self,
                           data: Dict[str, pd.DataFrame],
                           visuals: Dict[str, Any],
                           report: Report) -> str:
        """Generate HTML report"""
        template = self.template_env.get_template(
            f"{report.type.value}/report.html"
        )
        
        # Convert visuals to HTML
        visual_html = {}
        for name, fig in visuals.items():
            visual_html[name] = fig.to_html(
                full_html=False,
                include_plotlyjs=False
            )
        
        return template.render(
            report=report,
            data=data,
            visuals=visual_html
        )
    
    async def _generate_json(self,
                           data: Dict[str, pd.DataFrame],
                           report: Report) -> str:
        """Generate JSON report"""
        json_data = {}
        
        for key, df in data.items():
            json_data[key] = json.loads(
                df.to_json(orient='records')
            )
        
        return json.dumps({
            'report': {
                'id': report.id,
                'type': report.type.value,
                'name': report.name,
                'metadata': report.metadata,
                'created_at': report.created_at.isoformat(),
                'generated_at': datetime.utcnow().isoformat()
            },
            'data': json_data
        })

class ReportStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        
        if self.type == 's3':
            self.s3 = boto3.client('s3')
    
    async def store(self,
                   report: Report,
                   content: Union[bytes, str]):
        """Store report"""
        if self.type == 's3':
            await self._store_s3(report, content)
        elif self.type == 'file':
            await self._store_file(report, content)
    
    async def _store_s3(self,
                       report: Report,
                       content: Union[bytes, str]):
        """Store report in S3"""
        key = f"{report.type.value}/{report.id}.{report.format.value}"
        
        self.s3.put_object(
            Bucket=self.config['bucket'],
            Key=key,
            Body=content if isinstance(content, bytes) else content.encode(),
            ContentType=self._get_content_type(report.format)
        )
    
    async def _store_file(self,
                         report: Report,
                         content: Union[bytes, str]):
        """Store report in file system"""
        path = os.path.join(
            self.config['path'],
            report.type.value,
            f"{report.id}.{report.format.value}"
        )
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        mode = 'wb' if isinstance(content, bytes) else 'w'
        async with aiofiles.open(path, mode) as f:
            await f.write(content)
    
    def _get_content_type(self, format: ReportFormat) -> str:
        """Get content type for format"""
        content_types = {
            ReportFormat.PDF: 'application/pdf',
            ReportFormat.EXCEL: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            ReportFormat.CSV: 'text/csv',
            ReportFormat.HTML: 'text/html',
            ReportFormat.JSON: 'application/json'
        }
        return content_types.get(format, 'application/octet-stream')

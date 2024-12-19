from typing import Dict, Any, List, Optional, Callable
import asyncio
from datetime import datetime
import json
from dataclasses import dataclass
from abc import ABC, abstractmethod
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery

@dataclass
class DataPipelineConfig:
    name: str
    source_config: Dict[str, Any]
    transform_config: List[Dict[str, Any]]
    sink_config: Dict[str, Any]
    window_size: int = 60
    batch_size: int = 1000
    retry_count: int = 3

class DataTransform(ABC):
    @abstractmethod
    def transform(self, data: Any) -> Any:
        pass

class DataSource(ABC):
    @abstractmethod
    async def read(self) -> Any:
        pass

class DataSink(ABC):
    @abstractmethod
    async def write(self, data: Any):
        pass

class BeamPipeline:
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.pipeline = None
        self.options = PipelineOptions()
    
    def build_pipeline(self):
        """Build Apache Beam pipeline"""
        self.pipeline = beam.Pipeline(options=self.options)
        
        # Create pipeline steps
        data = (
            self.pipeline
            | "Read" >> self._create_source()
            | "Window" >> beam.WindowInto(
                window.FixedWindows(self.config.window_size)
            )
        )
        
        # Apply transformations
        for transform_config in self.config.transform_config:
            transform = self._create_transform(transform_config)
            data = data | transform_config["name"] >> transform
        
        # Write to sink
        data | "Write" >> self._create_sink()
    
    def _create_source(self) -> beam.PTransform:
        """Create data source based on configuration"""
        source_type = self.config.source_config["type"]
        
        if source_type == "pubsub":
            return ReadFromPubSub(
                subscription=self.config.source_config["subscription"]
            )
        elif source_type == "file":
            return beam.io.ReadFromText(
                self.config.source_config["path"]
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _create_transform(self, 
                         config: Dict[str, Any]) -> beam.PTransform:
        """Create transform based on configuration"""
        transform_type = config["type"]
        
        if transform_type == "filter":
            return beam.Filter(
                lambda x: eval(config["condition"], {"x": x})
            )
        elif transform_type == "map":
            return beam.Map(
                lambda x: eval(config["expression"], {"x": x})
            )
        else:
            raise ValueError(f"Unsupported transform type: {transform_type}")
    
    def _create_sink(self) -> beam.PTransform:
        """Create data sink based on configuration"""
        sink_type = self.config.sink_config["type"]
        
        if sink_type == "bigquery":
            return WriteToBigQuery(
                table=self.config.sink_config["table"],
                schema=self.config.sink_config["schema"]
            )
        elif sink_type == "file":
            return beam.io.WriteToText(
                self.config.sink_config["path"]
            )
        else:
            raise ValueError(f"Unsupported sink type: {sink_type}")
    
    def run(self):
        """Run the pipeline"""
        if not self.pipeline:
            self.build_pipeline()
        return self.pipeline.run()

class AsyncDataPipeline:
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.queue = asyncio.Queue(maxsize=config.batch_size)
        self.running = False
        self.transforms: List[DataTransform] = []
        self.source: Optional[DataSource] = None
        self.sink: Optional[DataSink] = None
    
    async def start(self):
        """Start the pipeline"""
        self.running = True
        await asyncio.gather(
            self._read_data(),
            self._process_data()
        )
    
    async def stop(self):
        """Stop the pipeline"""
        self.running = False
        # Wait for queue to be empty
        while not self.queue.empty():
            await asyncio.sleep(1)
    
    async def _read_data(self):
        """Read data from source"""
        while self.running:
            try:
                data = await self.source.read()
                await self.queue.put(data)
            except Exception as e:
                print(f"Error reading data: {e}")
                await asyncio.sleep(1)
    
    async def _process_data(self):
        """Process data through pipeline"""
        batch = []
        
        while self.running:
            try:
                # Collect batch
                while len(batch) < self.config.batch_size:
                    if self.queue.empty():
                        if batch:
                            break
                        await asyncio.sleep(0.1)
                        continue

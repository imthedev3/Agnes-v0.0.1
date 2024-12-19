from typing import Dict, Any, Optional, List
import asyncio
import time
from dataclasses import dataclass
import opentelemetry.trace as trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import contextvars
import uuid

@dataclass
class SpanContext:
    trace_id: str
    span_id: str
    parent_id: Optional[str] = None
    baggage: Optional[Dict[str, str]] = None

class TracingManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tracer_provider = TracerProvider()
        self.setup_exporters()
        trace.set_tracer_provider(self.tracer_provider)
        self.tracer = trace.get_tracer(__name__)
        self.propagator = TraceContextTextMapPropagator()
        self.current_span_ctx = contextvars.ContextVar(
            'current_span_ctx',
            default=None
        )
    
    def setup_exporters(self):
        """Setup trace exporters"""
        # Jaeger exporter
        jaeger_exporter = JaegerExporter(
            agent_host_name=self.config.get("jaeger_host", "localhost"),
            agent_port=self.config.get("jaeger_port", 6831),
        )
        
        self.tracer_provider.add_span_processor(
            BatchSpanProcessor(jaeger_exporter)
        )
    
    def create_span(self, 
                   name: str, 
                   parent_ctx: Optional[SpanContext] = None,
                   attributes: Optional[Dict[str, Any]] = None) -> SpanContext:
        """Create a new span"""
        context = trace.set_span_in_context(
            self.tracer.start_span(name)
        ) if not parent_ctx else parent_ctx
        
        span = self.tracer.start_span(
            name,
            context=context,
            attributes=attributes or {}
        )
        
        span_context = SpanContext(
            trace_id=format(span.get_span_context().trace_id, "032x"),
            span_id=format(span.get_span_context().span_id, "016x"),
            parent_id=parent_ctx.span_id if parent_ctx else None
        )
        
        self.current_span_ctx.set(span_context)
        return span_context
    
    def end_span(self):
        """End the current span"""
        if self.current_span_ctx.get():
            trace.get_current_span().end()
            self.current_span_ctx.set(None)
    
    def inject_context(self, headers: Dict[str, str]):
        """Inject trace context into headers"""
        self.propagator.inject(headers)
    
    def extract_context(self, headers: Dict[str, str]) -> Optional[SpanContext]:
        """Extract trace context from headers"""
        context = self.propagator.extract(headers)
        if not context:
            return None
        
        span = trace.get_current_span(context)
        if not span:
            return None
        
        return SpanContext(
            trace_id=format(span.get_span_context().trace_id, "032x"),
            span_id=format(span.get_span_context().span_id, "016x")
        )
    
    def trace(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Decorator for tracing functions"""
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                parent_ctx = self.current_span_ctx.get()
                span_ctx = self.create_span(
                    name,
                    parent_ctx,
                    attributes
                )
                
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    trace.get_current_span().set_attribute(
                        "error", True
                    )
                    trace.get_current_span().set_attribute(
                        "error.type", type(e).__name__
                    )
                    trace.get_current_span().set_attribute(
                        "error.message", str(e)
                    )
                    raise
                finally:
                    self.end_span()
            
            def sync_wrapper(*args, **kwargs):
                parent_ctx = self.current_span_ctx.get()
                span_ctx = self.create_span(
                    name,
                    parent_ctx,
                    attributes
                )
                
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    trace.get_current_span().set_attribute(
                        "error", True
                    )
                    trace.get_current_span().set_attribute(
                        "error.type", type(e).__name__
                    )
                    trace.get_current_span().set_attribute(
                        "error.message", str(e)
                    )
                    raise
                finally:
                    self.end_span()
            
            return async_wrapper if asyncio.iscoroutinefunction(func) \
                else sync_wrapper
        
        return decorator

class SpanBuilder:
    def __init__(self, tracer: TracingManager, name: str):
        self.tracer = tracer
        self.name = name
        self.attributes: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []
    
    def add_attribute(self, key: str, value: Any) -> 'SpanBuilder':
        """Add attribute to span"""
        self.attributes[key] = value
        return self
    
    def add_event(self, 
                  name: str, 
                  attributes: Optional[Dict[str, Any]] = None) -> 'SpanBuilder':
        """Add event to span"""
        self.events.append({
            'name': name,
            'attributes': attributes or {},
            'timestamp': time.time_ns()
        })
        return self
    
    def build(self) -> SpanContext:
        """Build and start the span"""
        span_ctx = self.tracer.create_span(
            self.name,
            attributes=self.attributes
        )
        
        # Add events
        span = trace.get_current_span()
        for event in self.events:
            span.add_event(
                name=event['name'],
                attributes=event['attributes'],
                timestamp=event['timestamp']
            )
        
        return span_ctx

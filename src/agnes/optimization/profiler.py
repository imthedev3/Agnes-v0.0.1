from typing import Dict, Any, Optional, List
import cProfile
import pstats
import time
import asyncio
import tracemalloc
from functools import wraps
from dataclasses import dataclass
import numpy as np

@dataclass
class ProfileResult:
    function_name: str
    execution_time: float
    memory_usage: int
    cpu_usage: float
    call_count: int

class PerformanceProfiler:
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.results: Dict[str, List[ProfileResult]] = {}
        tracemalloc.start()
    
    async def profile_function(self, func):
        """Decorator for profiling async functions"""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Start profiling
            start_time = time.time()
            memory_start = tracemalloc.get_traced_memory()[0]
            
            # Execute function
            self.profiler.enable()
            result = await func(*args, **kwargs)
            self.profiler.disable()
            
            # Collect metrics
            end_time = time.time()
            memory_end = tracemalloc.get_traced_memory()[0]
            
            # Create profile result
            profile_result = ProfileResult(
                function_name=func.__name__,
                execution_time=end_time - start_time,
                memory_usage=memory_end - memory_start,
                cpu_usage=self._get_cpu_usage(),
                call_count=1
            )
            
            # Store result
            if func.__name__ not in self.results:
                self.results[func.__name__] = []
            self.results[func.__name__].append(profile_result)
            
            return result
        
        return wrapper
    
    def _get_cpu_usage(self) -> float:
        """Get CPU usage for the current process"""
        import psutil
        process = psutil.Process()
        return process.cpu_percent()
    
    def get_stats(self, function_name: Optional[str] = None) -> Dict[str, Any]:
        """Get profiling statistics"""
        if function_name and function_name in self.results:
            results = self.results[function_name]
        else:
            results = [r for results in self.results.values() for r in results]
        
        if not results:
            return {}
        
        # Calculate statistics
        exec_times = [r.execution_time for r in results]
        memory_usage = [r.memory_usage for r in results]
        cpu_usage = [r.cpu_usage for r in results]
        
        return {
            "execution_time": {
                "mean": np.mean(exec_times),
                "std": np.std(exec_times),
                "min": np.min(exec_times),
                "max": np.max(exec_times),
            },
            "memory_usage": {
                "mean": np.mean(memory_usage),
                "std": np.std(memory_usage),
                "min": np.min(memory_usage),
                "max": np.max(memory_usage),
            },
            "cpu_usage": {
                "mean": np.mean(cpu_usage),
                "std": np.std(cpu_usage),
                "min": np.min(cpu_usage),
                "max": np.max(cpu_usage),
            },
            "call_count": len(results)
        }

class BottleneckDetector:
    def __init__(self, threshold_ms: float = 100):
        self.threshold_ms = threshold_ms
        self.slow_operations: List[Dict[str, Any]] = []
    
    async def monitor(self, func):
        """Decorator for monitoring function performance"""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = tracemalloc.get_traced_memory()[0]
            
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                self._record_error(func.__name__, e)
                raise
            
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            memory_delta = tracemalloc.get_traced_memory()[0] - start_memory
            
            if execution_time > self.threshold_ms:
                self._record_slow_operation(
                    func.__name__,
                    execution_time,
                    memory_delta
                )
            
            return result
        
        return wrapper
    
    def _record_slow_operation(self, 
                             func_name: str, 
                             execution_time: float, 
                             memory_delta: int):
        """Record slow operation details"""
        self.slow_operations.append({
            "function": func_name,
            "execution_time_ms": execution_time,
            "memory_delta": memory_delta,
            "timestamp": time.time(),
            "stack_trace": traceback.extract_stack()
        })
    
    def _record_error(self, func_name: str, error: Exception):
        """Record error details"""
        self.slow_operations.append({
            "function": func_name,
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": time.time(),
            "stack_trace": traceback.extract_tb(error.__traceback__)
        })
    
    def get_bottlenecks(self) -> List[Dict[str, Any]]:
        """Get list of detected bottlenecks"""
        return sorted(
            self.slow_operations,
            key=lambda x: x.get("execution_time_ms", 0),
            reverse=True
        )

class ResourceOptimizer:
    def __init__(self):
        self.memory_threshold = 0.8  # 80% memory usage threshold
        self.cpu_threshold = 0.8     # 80% CPU usage threshold
    
    async def optimize_resources(self):
        """Optimize system resources"""
        import psutil
        
        while True:
            # Check system resources
            memory_percent = psutil.virtual_memory().percent / 100
            cpu_percent = psutil.cpu_percent() / 100
            
            if memory_percent > self.memory_threshold:
                await self._optimize_memory()
            
            if cpu_percent > self.cpu_threshold:
                await self._optimize_cpu()
            
            await asyncio.sleep(60)  # Check every minute
    
    async def _optimize_memory(self):
        """Optimize memory usage"""
        import gc
        
        # Force garbage collection
        gc.collect()
        
        # Clear internal caches
        import sys
        sys.intern.clear()
        
        # Clear function cache if exists
        for func in gc.get_objects():
            if hasattr(func, "cache_clear"):
                func.cache_clear()
    
    async def _optimize_cpu(self):
        """Optimize CPU usage"""
        # Implement CPU optimization strategies
        # For example, adjust thread pool size, batch processing, etc.
        pass

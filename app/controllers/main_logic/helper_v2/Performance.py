import line_profiler
import pickle

DEBUG=True

def timer_decorator(func):
    if DEBUG:
        def wrapper(*args, **kwargs):
            profiler = line_profiler.LineProfiler()
            profiler.add_function(func)
            profiler.enable_by_count()
            result = func(*args, **kwargs)
            profiler.disable_by_count()
            profiler.print_stats()
            
            return result
        return wrapper

def timer_decorator_get_data(*args, **kwargs):
    if DEBUG:
        def wrapper(func):
            profiler = line_profiler.LineProfiler()
            profiler.add_function(func)
            profiler.enable_by_count()
            result = func(*args, **kwargs)
            profiler.disable_by_count()
            profiler.print_stats()
            profiler.dump_stats(f"{args[0]}/data")
            
                
            return result
            
        return wrapper
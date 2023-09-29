from pathlib import Path 

from cydonia.profiler.CacheTrace import CacheTrace
from keyuri.config.Config import GlobalConfig


class CreateAccessTrace:
    def __init__(
            self,
            stack_binary_path: str,
            global_config: GlobalConfig = GlobalConfig()
    ) -> None:
        """This class generates access traces from block storage traces. 
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = global_config
        self._stack_binary_path = stack_binary_path
    

    def create(
            self,
            blk_trace_path: Path,
            access_trace_path: Path
    ) -> None:
        cache_trace = CacheTrace(self._stack_binary_path)
        access_trace_path.parent.mkdir(exist_ok=True, parents=True)
        cache_trace.generate_access_trace(str(blk_trace_path), str(access_trace_path))
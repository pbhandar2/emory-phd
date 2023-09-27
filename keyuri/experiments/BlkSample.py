"""This class runs the BlkSample algorithm on all the samples that it can find.
"""

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class BlkSample:
    def __init__(
            self,
            global_config: GlobalConfig = GlobalConfig(),
            sample_config: SampleExperimentConfig = SampleExperimentConfig()
    ) -> None:
        self._global_config = global_config
        self._sample_config = sample_config 
    

    def get_sample_file_path_arr(self) -> list:
        sample_file_path_arr = []
        
        return sample_file_path_arr
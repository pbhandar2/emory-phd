from copy import deepcopy
from pathlib import Path 
from itertools import product 
from json import load 


class GlobalConfig:
    representative_cp_workloads_arr = ["w09", "w18", "w64", "w66", "w92"]
    
    source_dir_path = Path("/research2/mtc/cp_traces/pranav/")

    metadata_dir_path = source_dir_path.joinpath("meta")

    block_trace_dir_path = source_dir_path.joinpath("block_traces")
    cache_trace_dir_path = source_dir_path.joinpath("block_cache_trace")
    sample_block_trace_dir_path = source_dir_path.joinpath("sample_block_traces")
    sample_cache_trace_dir_path = source_dir_path.joinpath("sample_block_cache_traces")

    block_feature_dir_path = metadata_dir_path.joinpath("block_features")
    cache_feature_dir_path = metadata_dir_path.joinpath("cache_features")
    cumulative_features_dir_path = metadata_dir_path.joinpath("cumulative_features")
    sample_block_feature_dir_path = metadata_dir_path.joinpath("sample", "block_features")
    sample_cache_features_dir_path = metadata_dir_path.joinpath("sample", "cache_features")
    sample_cumulative_features_dir_path = metadata_dir_path.joinpath("sample", "cumulative_features")

    rd_hist_dir_path = metadata_dir_path.joinpath("rd_hist")
    sample_rd_hist_dir_path = metadata_dir_path.joinpath("sample_rd_hist")

    sample_split_feature_dir_path = metadata_dir_path.joinpath("sample", "split_features")
    sample_percent_diff_feature_dir_path = metadata_dir_path.joinpath("sample", "percent_diff_features")


    def update_source_dir(self, new_source_dir:str):
        self.source_dir_path = Path(new_source_dir)

        self.metadata_dir_path = self.source_dir_path.joinpath("meta")

        self.block_trace_dir_path = self.source_dir_path.joinpath("block_traces")
        self.cache_trace_dir_path = self.source_dir_path.joinpath("block_cache_trace")
        self.block_feature_dir_path = self.metadata_dir_path.joinpath("block_features")
        self.cache_feature_dir_path = self.metadata_dir_path.joinpath("cache_features")
        self.rd_hist_dir_path = self.metadata_dir_path.joinpath("rd_hist")

        self.sample_rd_hist_dir_path = self.metadata_dir_path.joinpath("sample_rd_hist")
        self.sample_block_trace_dir_path = self.source_dir_path.joinpath("sample_block_traces")
        self.sample_cache_trace_dir_path = self.source_dir_path.joinpath("sample_block_cache_traces")

        self.sample_block_feature_dir_path = self.metadata_dir_path.joinpath("sample", "block_features")
        self.sample_cache_features_dir_path = self.metadata_dir_path.joinpath("sample", "cache_features")
        self.sample_split_feature_dir_path = self.metadata_dir_path.joinpath("sample", "split_features")
        self.sample_percent_diff_feature_dir_path = self.metadata_dir_path.joinpath("sample", "percent_diff_features")


    def get_block_trace_path(
            self,
            workload_type: str, 
            workload_name: str 
    ) -> Path:
        return self.block_trace_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
    

    def get_block_cache_trace_path(
            self,
            workload_type: str, 
            workload_name: str 
    ) -> Path:
        return self.cache_trace_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
    

    def get_block_feature_file_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        feature_dir = self.block_feature_dir_path
        return feature_dir.joinpath(
                workload_type,
                "{}.json".format(workload_name)
        )


    def get_rd_hist_file_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        feature_dir = self.rd_hist_dir_path
        return feature_dir.joinpath(
                workload_type,
                "{}.csv".format(workload_name)
        )
    

    def get_cache_feature_file_path(
            self,
            workload_type: str,
            workload_name: str,
    ) -> Path:
        feature_dir = self.cache_feature_dir_path
        return feature_dir.joinpath(
                workload_type,
                "{}.json".format(workload_name)
        )
    

    def get_cumulative_feature_file_path(
            self,
            workload_type: str,
            workload_name: str,
    ) -> Path:
        feature_dir = self.cumulative_features_dir_path
        return feature_dir.joinpath(
                workload_type,
                "{}.json".format(workload_name)
        )





class BaseMTExperiment:
	def __init__(self):
		self._workloads = {
			"cp": ["w66", "w09", "w18", "w64", "w92"]
		}

		self._replay_rate_arr = [3, 2, 1]
		self._cache_size_ratio_arr = [0.1, 0.2, 0.4, 0.6]
		self._max_pending_block_req_count = 128
		self._num_async_threads = 18 
		self._num_block_threads = 18 
		self._min_t1_size_mb = 100 
		self._min_t2_size_mb = 150 
    

	def get_wss_byte(
		self, 
		workload_type: str, 
		workload_name: str,
        global_config: GlobalConfig = GlobalConfig()
	) -> int:
		feature_file_path = global_config.get_block_feature_file_path(workload_type, workload_name)
		with feature_file_path.open("r") as feature_file_handle:
			feature_dict = load(feature_file_handle)
		return feature_dict["wss"]
        
	
	def get_all_replay_config(
		self,
        global_config: GlobalConfig = GlobalConfig()
    ) -> list:
		"""Get the list of all replay configurations for base MT experiments.
          
        Returns:
			replay_config_list: List of dictionaries of replay configuration. 
		"""
		replay_config_list = []
		for workload_type, workload_name in self.get_workloads():
			wss_byte = self.get_wss_byte(workload_type, workload_name)
			workload_replay_config_list = self.get_replay_config_list_per_workload(wss_byte)
			block_trace_path = global_config.get_block_cache_trace_path(workload_type, workload_name)
			for replay_config in workload_replay_config_list:
				replay_config["block_trace_path"] = block_trace_path
				replay_config_list.append(replay_config)
				print(replay_config)
				print(workload_name)
		return replay_config_list
        

	def get_workloads(self) -> list:
		"""Get a list of tuples of (workload type, workload name) of all relevant workloads.

		Returns:
			workload_arr: Array of tuples containing two string values (workload type, workload name).
		"""
		workload_arr = []
		for workload_type in self._workloads:
			workload_name_arr = self._workloads[workload_type]
			for workload_name in workload_name_arr: 
				workload_arr.append((workload_type, workload_name))
		return workload_arr 
	

	def get_replay_config_list_per_workload(
		self,
        wss_byte: int
    ) -> None:
		"""Get the list of replay configurations based on the working set size of the workload.

		Args:
			wss_byte: Working set size of workload in bytes. 

		Returns:
			config_arr: Array of dictionaries containing replay configurations.  
		"""
		replay_config = {
			"max_pending_block_req_count": self._max_pending_block_req_count,
            "num_async_threads": self._num_async_threads,
            "num_block_threads": self._num_block_threads
		}
		replay_config_arr = [] 
		size_arr = [(cache_size_ratio * wss_byte)//(1024*1024) for cache_size_ratio in self._cache_size_ratio_arr]
		for replay_rate in self._replay_rate_arr:
			for t1_size_mb in size_arr:
				cur_replay_config = deepcopy(replay_config)
				cur_replay_config["replay_rate"] = replay_rate 
				cur_replay_config["t1_size_mb"] = t1_size_mb 
				cur_replay_config["t2_size_mb"] = 0 
				replay_config_arr.append(cur_replay_config)
				for t2_size_mb in size_arr:
					cur_replay_config = deepcopy(replay_config)
					cur_replay_config["replay_rate"] = replay_rate 
					cur_replay_config["t1_size_mb"] = t1_size_mb 
					cur_replay_config["t2_size_mb"] = t2_size_mb 
					replay_config_arr.append(cur_replay_config)
		return replay_config_arr
        

class NodeConfig:
        def __init__(self):
             self.machine_dict = {
                  "c220g1": {
                       "max_t1_size_mb": 110000,
                       "max_t2_size_mb": 400000
                  },
                  "c220g5": {
                       "max_t1_size_mb": 110000,
                       "max_t2_size_mb": 400000
                  },
                  "r6525": {
                       "max_t1_size_mb": 110000,
                       "max_t2_size_mb": 400000
                  }
             }


    

class SampleExperimentConfig:
    rate_arr = [1, 5, 10, 20, 40, 80]
    seed_arr = [42, 43, 44]
    bits_arr = [0, 2, 4, 6, 8, 10, 12]  
    sample_type = "iat"


    def get_sample_trace_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        trace_dir = global_config.sample_block_trace_dir_path
        return trace_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.csv".format(rate, bits, seed)
        )
    

    def get_sample_cache_trace_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        trace_dir = global_config.sample_cache_trace_dir_path
        return trace_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.csv".format(rate, bits, seed)
        )
    

    def get_split_feature_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_split_feature_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
    

    def get_block_feature_file_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_block_feature_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
    

    def get_cache_feature_file_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_cache_features_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
    

    def get_percent_diff_feature_file_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_percent_diff_feature_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
    
    def get_rd_hist_file_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_rd_hist_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
    

    def get_cumulative_feature_file_path(
            self,
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        feature_dir = global_config.sample_cumulative_features_dir_path
        return feature_dir.joinpath(
                sample_type,
                workload_type,
                workload_name,
                "{}_{}_{}.json".format(rate, bits, seed)
        )
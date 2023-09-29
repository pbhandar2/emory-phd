from copy import deepcopy
from pathlib import Path
from json import load


class GlobalConfig:
    def __init__(
            self, 
            source_dir_path: Path = Path("/research2/mtc/cp_traces/pranav/")
    ) -> None:
        self.representative_cp_workloads_arr = ["w09", "w18", "w64", "w66", "w92"]
        self.source_dir_path = source_dir_path

        # original, sample and postprocessed block, cache and access traces 
        # block trace contains block request with format: ts, lba, op, size 
        # cache trace have block requests broken into fixed-sized 4KB requests and compute its rd with format: ts, blk_id, op, rd
        # block access trace also have block request broken into fixed-sized blocks but it does not consider misalignment same format as cache trace
        self.block_trace_dir_path = self.source_dir_path.joinpath("block_traces")
        self.block_cache_trace_dir_path = self.source_dir_path.joinpath("block_cache_trace")
        self.block_access_trace_dir_path = self.source_dir_path.joinpath("block_access_trace")
        
        self.sample_block_trace_dir_path = self.source_dir_path.joinpath("sample_block_traces")
        self.sample_cache_trace_dir_path = self.source_dir_path.joinpath("sample_block_cache_traces")
        self.sample_block_access_trace_dir_path = self.source_dir_path.joinpath("sample_block_access_traces")

        self.postprocess_sample_block_trace_dir_path = self.source_dir_path.joinpath("postprocess_sample_block_traces")
        self.postprocess_sample_cache_trace_dir_path = self.source_dir_path.joinpath("postprocess_sample_block_cache_traces")
        self.postprocess_sample_block_access_trace_dir_path = self.source_dir_path.joinpath("postprocess_block_access_traces")

        # directory for each type of metadata 
        # block_features: Fetures generated from the block trace. 
        # cache_features: Feature related to hit rate generated from cache trace 
        # access_features: Features related to hit rate fenerated from access trace 
        # cumulative_features: All features of a workload cumulated in one JSON file for each workload. 
        # rd_hist: Reuse distance histogram generated from block traces. 
        # access_rd_hist: Reuse distance histogram generated from access traces. 
        self.metadata_dir_path = self.source_dir_path.joinpath("meta")
        self.block_feature_dir_path = self.metadata_dir_path.joinpath("block_features")
        self.cache_feature_dir_path = self.metadata_dir_path.joinpath("cache_features")
        self.cumulative_features_dir_path = self.metadata_dir_path.joinpath("cumulative_features")
        self.rd_hist_dir_path = self.metadata_dir_path.joinpath("rd_hist")
        self.access_rd_hist_dir_path = self.metadata_dir_path.joinpath("access_rd_hist")

        # metadata features for samples 
        self.sample_block_feature_dir_path = self.metadata_dir_path.joinpath("sample", "block_features")
        self.sample_cache_features_dir_path = self.metadata_dir_path.joinpath("sample", "cache_features")
        self.sample_cumulative_features_dir_path = self.metadata_dir_path.joinpath("sample", "cumulative_features")
        self.sample_rd_hist_dir_path = self.metadata_dir_path.joinpath("sample", "rd_hist")
        self.sample_access_rd_hist_dir_path = self.metadata_dir_path.joinpath("sample", "access_rd_hist")

        # split_features: metadata about the number of splits during sampling. 
        # percent_diff_fetures: percentage difference across all features in the sample and full trace 
        self.sample_split_feature_dir_path = self.metadata_dir_path.joinpath("sample", "split_features")
        self.sample_percent_diff_feature_dir_path = self.metadata_dir_path.joinpath("sample", "percent_diff_features")

        # per_iteration_error: The output of each iteration of post-processing algorithms.
        self.postprocess_per_iteration_error_dir_path = self.metadata_dir_path.joinpath("postprocess", "per_iteration_error")
        # overall: Overall error of post-processing algorithms.
        self.postprocess_stat_dir_path = self.metadata_dir_path.joinpath("postprocess", "overall")
        self.postprocess_rd_hist_dir_path = self.metadata_dir_path.joinpath("postprocess", "rd_hist")
        self.postprocess_access_rd_hist_dir_path = self.metadata_dir_path.joinpath("postprocess", "access_rd_hist")
        # hrc_error: Difference in HRC between postprocessed, sampled and full trace. 
        self.postprocess_hrc_err_dir_path = self.metadata_dir_path.joinpath("postprocess", "hrc_err")
        # replay: Output of block trace replay. 
        self.replay_dir_path = self.source_dir_path.joinpath("replay")
        # replay_backup: Stores backups of the replay directory. 
        self.replay_backup_dir_path = self.source_dir_path.joinpath("replay_backup")

        # remote_output_dir: Directory in the remote node where output of replay will be stored. 
        self.remote_output_dir = "/run/replay"
        # set of files generated after block trace replay. 
        self.replay_output_file_list = [
            "{}/config.json".format(self.remote_output_dir),
            "{}/usage.csv".format(self.remote_output_dir),
            "{}/power.csv".format(self.remote_output_dir),
            "{}/tsstat_0.out".format(self.remote_output_dir),
            "{}/stat_0.out".format(self.remote_output_dir),
            "{}/stdout.dump".format(self.remote_output_dir),
            "{}/stderr.dump".format(self.remote_output_dir)
        ]


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
        return self.block_cache_trace_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
    

    def get_access_trace_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        return self.block_access_trace_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
    

    def get_replay_output_dir_path(
        self, 
        machine_name: str, 
        replay_config: dict 
    ) -> Path:
        replay_output_dir_path = self.replay_dir_path.joinpath(machine_name, replay_config["workload_type"], replay_config["workload_name"])
        experiment_name = "q={}_".format(replay_config["max_pending_block_req_count"])
        experiment_name += "bt={}_".format(replay_config["num_block_threads"])
        experiment_name += "at={}_".format(replay_config["num_async_threads"])
        experiment_name += "t1={}_".format(replay_config["t1_size_mb"])
        experiment_name += "t2={}_".format(replay_config["t2_size_mb"])
        experiment_name += "rr={}_".format(replay_config["replay_rate"])
        experiment_name += "it={}".format(replay_config["iteration"])
        return replay_output_dir_path.joinpath(experiment_name)
    

    def get_block_feature_file_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        return self.block_feature_dir_path.joinpath(workload_type, "{}.json".format(workload_name))


    def get_rd_hist_file_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        return self.rd_hist_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
    

    def get_access_rd_hist_file_path(
            self,
            workload_type: str,
            workload_name: str
    ) -> Path:
        return self.access_rd_hist_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))


    def get_cache_feature_file_path(
            self,
            workload_type: str,
            workload_name: str,
    ) -> Path:
        return self.cache_feature_dir_path.joinpath(workload_type, "{}.json".format(workload_name))


    def get_cumulative_feature_file_path(
            self,
            workload_type: str,
            workload_name: str,
    ) -> Path:
        return self.cumulative_features_dir_path.joinpath(workload_type, "{}.json".format(workload_name))


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
					block_trace_path = global_config.get_block_trace_path(workload_type, workload_name)
					assert block_trace_path.exists(), "Block trace {} does not exist.".format(block_trace_path)
					for replay_config in workload_replay_config_list:
							replay_config["block_trace_path"] = block_trace_path
							replay_config["workload_type"] = workload_type
							replay_config["workload_name"] = workload_name
							replay_config_list.append(replay_config)
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
    ) -> list:
		"""Get the list of replay configurations based on the working set size of the workload.

		Args:
				wss_byte: Working set size of workload in bytes.

		Returns:
				config_arr: Array of dictionaries containing replay configurations.
		"""
		replay_config = {
			"max_pending_block_req_count": self._max_pending_block_req_count,
			"num_async_threads": self._num_async_threads,
			"num_block_threads": self._num_block_threads,
			"iteration": 0,
			"sample": 0
		}
		replay_config_arr = []
		size_arr = [(cache_size_ratio * wss_byte)//(1024*1024) for cache_size_ratio in self._cache_size_ratio_arr]
		for replay_rate in self._replay_rate_arr:
			for t1_size_mb in size_arr:
				cur_replay_config = deepcopy(replay_config)
				cur_replay_config["replay_rate"] = int(replay_rate)
				cur_replay_config["t1_size_mb"] = int(t1_size_mb)
				cur_replay_config["t2_size_mb"] = 0
				replay_config_arr.append(cur_replay_config)
				for t2_size_mb in size_arr:
					cur_replay_config = deepcopy(replay_config)
					cur_replay_config["replay_rate"] = int(replay_rate)
					cur_replay_config["t1_size_mb"] = int(t1_size_mb)
					cur_replay_config["t2_size_mb"] = int(t2_size_mb)
					replay_config_arr.append(cur_replay_config)
		return replay_config_arr


class NodeConfig:
	def __init__(self):
		self.machine_dict = {
			"c220g1": {
				"max_t1_size_mb": 110000,
				"max_t2_size_mb": 400000,
				"min_t1_size_mb": 100,
				"min_t2_size_mb": 150
			},
			"c220g5": {
				"max_t1_size_mb": 110000,
				"max_t2_size_mb": 400000,
				"min_t1_size_mb": 100,
				"min_t2_size_mb": 150
			},
			"r6525": {
				"max_t1_size_mb": 110000,
				"max_t2_size_mb": 400000,
				"min_t1_size_mb": 100,
				"min_t2_size_mb": 150
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


    def get_per_iteration_error_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        data_dir = global_config.postprocess_per_iteration_error_dir_path
        return data_dir.joinpath(postprocess_type,
                                    sample_type,
                                    workload_type,
                                    workload_name,
                                    "{}_{}_{}.csv".format(rate, bits, seed))


    def get_post_process_metadata_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        data_dir = global_config.postprocess_stat_dir_path
        return data_dir.joinpath(postprocess_type,
                                    sample_type,
                                    workload_type,
                                    workload_name,
                                    "{}_{}_{}.json".format(rate, bits, seed))
    

    def get_post_process_sample_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        data_dir = global_config.postprocess_sample_block_trace_dir_path
        return data_dir.joinpath(postprocess_type,
                                    sample_type,
                                    workload_type,
                                    workload_name,
                                    "{}_{}_{}.csv".format(rate, bits, seed))


    def get_post_process_rd_hist_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int,
            global_config = GlobalConfig()
    ) -> Path:
        data_dir = global_config.postprocess_rd_hist_dir_path
        return data_dir.joinpath(postprocess_type,
                                    sample_type,
                                    workload_type,
                                    workload_name,
                                    "{}_{}_{}.csv".format(rate, bits, seed))
    
from pathlib import Path 


class GlobalConfig:
    representative_cp_workloads_arr = ["w09", "w18", "w64", "w66", "w92"]
    
    source_dir_path = Path("/research2/mtc/cp_traces/pranav/")

    metadata_dir_path = source_dir_path.joinpath("meta")

    block_trace_dir_path = source_dir_path.joinpath("block_traces")
    cache_trace_dir_path = source_dir_path.joinpath("block_cache_trace")
    block_feature_dir_path = metadata_dir_path.joinpath("block_features")
    cache_feature_dir_path = metadata_dir_path.joinpath("cache_features")

    sample_block_trace_dir_path = source_dir_path.joinpath("sample_block_traces")
    sample_cache_trace_dir_path = source_dir_path.joinpath("sample_block_cache_traces")
    sample_block_feature_dir_path = metadata_dir_path.joinpath("sample", "block_features")
    sample_cache_features_dir_path = metadata_dir_path.joinpath("sample", "cache_features")

    sample_split_feature_dir_path = metadata_dir_path.joinpath("sample", "split_features")
    sample_percent_diff_feature_dir_path = metadata_dir_path.joinpath("sample", "percent_diff_features")


    def update_source_dir(self, new_source_dir:str):
        self.source_dir_path = Path(new_source_dir)

        self.metadata_dir_path = self.source_dir_path.joinpath("meta")

        self.block_trace_dir_path = self.source_dir_path.joinpath("block_traces")
        self.cache_trace_dir_path = self.source_dir_path.joinpath("block_cache_trace")
        self.block_feature_dir_path = self.metadata_dir_path.joinpath("block_features")
        self.cache_feature_dir_path = self.metadata_dir_path.joinpath("cache_features")

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
        print("sample trace dir is {}, workload name: {}".format(trace_dir, workload_name))
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
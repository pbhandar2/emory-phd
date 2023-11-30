from pathlib import Path

DEFAULT_SOURCE_DIR_PATH = Path("/research2/mtc/cp_traces/pranav-phd")

"""BaseConfig stores the directory of files used in the experiments. 

There are 3 types of trace files: base, sample and processed. The base traces
represent the original trace files. The sample traces represent the trace
generated from sampling the base traces. The processed traces are generated
by processing the sample traces. 

Each block trace file generates a cache trace, rd trace and a rd hist file.
The cache trace is a trace of request to fixed sized cache blocks. The rd trace
is a trace of reuse distance of each request in cache trace. The rd hist file
contains the reuse distance count generated from the rd trace. 
"""
class BaseConfig:
    def __init__(
            self,
            workload_set_name: str = "cp",
            source_dir_path: Path = DEFAULT_SOURCE_DIR_PATH
    ) -> None:
        self._source = source_dir_path
        self._workload_set_name = workload_set_name
        self._block_trace_dir_path = self._source.joinpath(workload_set_name, "block_traces")
        self._cache_trace_dir_path = self._source.joinpath(workload_set_name, "cache_traces")
        self._rd_trace_dir_path = self._source.joinpath(workload_set_name, "rd_traces")
        self._rd_hist_dir_path = self._source.joinpath(workload_set_name, "rd_hists")
        self._cache_features_dir_path = self._source.joinpath(workload_set_name, "cache_features")
        self._miss_rate_error_data_dir_path = self._source.joinpath(workload_set_name, "hit_rate_error")
        self._per_iteration_output_dir_path = self._source.joinpath(workload_set_name, "per_iteration_output")
        self._post_process_algo_output_dir_path = self._source.joinpath(workload_set_name, "post_process")

    
    def get_block_trace_path(self, workload_name: str) -> Path:
        return self._block_trace_dir_path.joinpath("{}.csv".format(workload_name))


    def get_all_block_traces(self) -> list:
        return list(self._block_trace_dir_path.iterdir())


    def get_cache_trace_path(self, workload_name: str) -> Path:
        return self._cache_trace_dir_path.joinpath("{}.csv".format(workload_name))
    

    def get_all_cache_traces(self) -> list:
        return list(self._cache_trace_dir_path.iterdir())
    

    def get_cache_features_path(self, workload_name: str) -> Path:
        return self._cache_features_dir_path.joinpath("{}.csv".format(workload_name))


    def get_rd_trace_path(self, workload_name: str) -> Path:
        return self._rd_trace_dir_path.joinpath("{}.csv".format(workload_name))
    

    def get_rd_hist_file_path(self, workload_name: str) -> Path:
        return self._rd_hist_dir_path.joinpath("{}.csv".format(workload_name))
    

    def get_compound_workload_set_name(self, sample_set_name: str, process_set_name: str = '') -> str:
        return "{}--{}".format(self._workload_set_name, sample_set_name) if not process_set_name else "{}--{}--{}".format(self._workload_set_name, sample_set_name, process_set_name)
    

    def get_sample_block_trace_path(
            self, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._block_trace_dir_path.joinpath(workload_name, "{}_{}_{}.csv".format(rate, bits, seed))
    

    def get_sample_cache_trace_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._cache_trace_dir_path.joinpath(workload_name, "{}_{}_{}.csv".format(rate, bits, seed))


    def get_all_sample_cache_traces(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> list:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return list(new_base_config._cache_trace_dir_path.joinpath(workload_name).iterdir())


    def get_sample_cache_features_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._cache_features_dir_path.joinpath(workload_name, "{}_{}_{}.csv".format(rate, bits, seed))
    

    def get_all_sample_cache_features(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> list:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return list(new_base_config._cache_features_dir_path.joinpath(workload_name).iterdir())
    

    def get_sample_rd_trace_path(
            self, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._rd_trace_dir_path.joinpath(workload_name, "{}_{}_{}.csv".format(rate, bits, seed))
    

    def get_sample_rd_hist_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._rd_hist_dir_path.joinpath(workload_name)
    

    def get_hit_rate_error_data_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._miss_rate_error_data_dir_path.joinpath(workload_name)
    

    def get_sample_rd_hist_file_path(
            self, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        return self.get_sample_rd_hist_dir_path(sample_set_name, workload_name).joinpath("{}_{}_{}.csv".format(rate, bits, seed))


    def get_processed_block_trace_path(self, process_set_name: str, sample_set_name: str, workload_name: str) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name, process_set_name=process_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name and "--" not in process_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config.get_block_trace_path(workload_name)


    def get_processed_cache_trace_path(self, process_set_name: str, sample_set_name: str, workload_name: str) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name, process_set_name=process_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name and "--" not in process_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config.get_cache_trace_path(workload_name)
    

    def get_processed_rd_trace_path(self, process_set_name: str, sample_set_name: str, workload_name: str) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name, process_set_name=process_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name and "--" not in process_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config.get_rd_trace_path(workload_name)
    

    def get_processed_rd_hist_file_path(self, process_set_name: str, sample_set_name: str, workload_name: str) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name, process_set_name=process_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name and "--" not in process_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config.get_rd_hist_file_path(workload_name)
    

    def get_per_iteration_output_file_path(
            self, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int,
            algo_bits: int 
    ):
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._per_iteration_output_dir_path.joinpath(workload_name, "{}_{}_{}_{}.csv".format(rate, bits, seed, algo_bits))


    def get_algo_output_file_path(
            self, 
            algo_name: str, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int,
            algo_bits: int
    ):
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_algo_output_dir_path.joinpath(algo_name, workload_name, "{}_{}_{}_{}.csv".format(rate, bits, seed, algo_bits))
""" BaseConfig returns the path of different files required for experiments. The BaseConfig
class takes a path which is the source directory and a string which is the workload
type as optional inputs during initiation. 

The subdirectories in the source directory contains data for each workload
type and its samples. For a given source directory (sourceDir), workload type (workloadType), 
its relevant data is stored in *sourceDir*/*workloadType*/. For sample type (sampleType),
the relevant data is stored *sourceDir*/*workloadType*--*sampleType*. 

For instance, given a source directory "/src" and workload type
"test", the subdirectory where data is stored is:
- src/test: The directory containing all data pertinent to workload type "test". 

Now to find data for samples of type "eff" for workload type "test" in the source dir "/src",
the subdirectory where data is stored is:
- src/test--eff: The directory containing all data pertinent to sample type "eff" generated 
                    from workload type "test".

Each original (non-sample) block trace in directory *sourceDir*/*workloadType*/block_traces generates the following files:
- cache_traces: A trace of accesses of fixed-sized data blocks. 
- rd_traces: List of reuse distances of each fixed-sized data block access in the cache trace. 
- rd_hists: Histogram of reuse distances generated from the rd trace.
- cache_features: Workload features generated from the cache trace. 

Each sample block trace in directory *sourceDir*/*workloadType*--*sampleType*/block_traces generates the following files 
in addition to the files listed above:
- sample_block_err: The sampling error across different features. The starting error before the samples are post-processed.
- hit_rate_err: The histograms of reuse distances of source block traces in *sourceDir*/*workloadType*/rd_hist and
                    sample block traces in *sourceDir*/*workloadType*--*sampleType*/rd_hist are compared to generate
                    the hit rate error files. 
- post_process_block_err: The output of error in block features of post processing algorithm after each iteration.
- post_process_cache_traces: The new cache traces generated based on the blocks removed during post-processing. 
- post_process_hit_rate_err: The output of error in hit rate of post processing algorithm after each iteration.
    - post_process_hit_rate_err/
"""

from pathlib import Path

DEFAULT_SOURCE_DIR_PATH = Path("/research2/mtc/cp_traces/pranav-phd")

def get_all_cp_workloads():
    return ["w{}".format(i) if i > 9 else "w0{}".format(i) for i in range(106, 0, -1)]


class BaseConfig:
    def __init__(
            self,
            workload_set_name: str = "cp",
            source_dir_path: Path = DEFAULT_SOURCE_DIR_PATH
    ) -> None:
        self._source = source_dir_path
        self._workload_set_name = workload_set_name

        # Directories used by both original (full) and sample block traces
        self._block_trace_dir_path = self._source.joinpath(workload_set_name, "block_traces")
        self._cache_trace_dir_path = self._source.joinpath(workload_set_name, "cache_traces")
        self._rd_trace_dir_path = self._source.joinpath(workload_set_name, "rd_traces")
        self._rd_hist_dir_path = self._source.joinpath(workload_set_name, "rd_hists")
        self._cache_features_dir_path = self._source.joinpath(workload_set_name, "cache_features")
        self._sample_hash_dir_path = self._source.joinpath(workload_set_name, "sample_hash_files")

        # Directories used by only sample block traces 
        self._block_error_dir_path = self._source.joinpath(workload_set_name, "block_error")
        self._access_features_dir_path = self._source.joinpath(workload_set_name, "access_features_files")
        self._miss_rate_error_data_dir_path = self._source.joinpath(workload_set_name, "hit_rate_error")
        self._per_iteration_output_dir_path = self._source.joinpath(workload_set_name, "per_iteration_output")
        self._sample_feature_output_dir_path = self._source.joinpath(workload_set_name, "sampling_features")
        self._cum_hr_error_data_dir_path = self._source.joinpath(workload_set_name, "cum_hr_error")

        self._post_process_algo_output_dir_path = self._source.joinpath(workload_set_name, "post_process")
        self._post_process_cache_trace_dir_path = self._source.joinpath(workload_set_name, "post_process_cache_trace")
        self._post_process_rd_trace_dir_path = self._source.joinpath(workload_set_name, "post_process_rd_trace")
        self._post_process_rd_hist_dir_path = self._source.joinpath(workload_set_name, "post_process_rd_hist")
        self._post_process_hit_rate_error_dir_path = self._source.joinpath(workload_set_name, "post_process_hit_rate_error")


    def get_compound_workload_set_name(self, sample_set_name: str) -> str:
        return "{}--{}".format(self._workload_set_name, sample_set_name)
    
    
    def get_block_trace_dir_path(self) -> Path:
        return self._source.joinpath(self._workload_set_name, "block_traces")


    def get_block_trace_path(self, workload_name: str) -> Path:
        return self.get_block_trace_dir_path().joinpath("{}.csv".format(workload_name))
    

    def get_all_block_traces(self) -> list:
        return list(self.get_block_trace_dir_path().iterdir())


    def get_cache_trace_dir_path(self) -> Path:
        return self._source.joinpath(self._workload_set_name, "cache_traces")
    

    def get_cache_trace_path(self, workload_name: str) -> Path:
        return self.get_cache_trace_dir_path().joinpath("{}.csv".format(workload_name))


    def get_all_cache_traces(self) -> list:
        return list(self.get_cache_trace_dir_path().iterdir())
    

    def get_rd_trace_dir_path(self) -> Path:
        return self._source.joinpath(self._workload_set_name, "rd_traces")
    

    def get_rd_trace_path(self, workload_name: str) -> Path:
        return self.get_rd_trace_dir_path().joinpath("{}.csv".format(workload_name))
    

    def get_all_rd_traces(self) -> list:
        return list(self.get_rd_trace_dir_path().iterdir())


    def get_rd_hist_dir_path(self) -> Path:
        return self._source.joinpath(self._workload_set_name, "rd_hists")
    

    def get_rd_hist_file_path(self, workload_name: str) -> Path:
        return self.get_rd_hist_dir_path().joinpath("{}.csv".format(workload_name))
    

    def get_all_rd_hists(self) -> list:
        return list(self.get_rd_hist_dir_path().iterdir())
    

    def get_cache_feature_dir_path(self) -> Path:
        return self._source.joinpath(self._workload_set_name, "cache_features")
    

    def get_cache_features_path(self, workload_name: str) -> Path:
        return self.get_cache_feature_dir_path().joinpath("{}.json".format(workload_name))
    

    def get_all_cache_features(self) -> list:
        return list(self.get_cache_feature_dir_path().iterdir())
    

    def get_sample_hash_dir_path(self) -> Path:
        return self._sample_hash_dir_path
    

    def get_sample_hash_file_path(self, workload_name: str, random_seed: int, num_lower_addr_bits_ignored: int) -> Path:
        return self.get_sample_hash_dir_path().joinpath("{}_{}_{}.csv".format(workload_name, random_seed, num_lower_addr_bits_ignored))
    

    def get_all_sample_hash_files(self) -> list:
        return list(self.get_sample_hash_dir_path().iterdir())
    

    """ --------------------------------------------- Sample helpers ---------------------------------------------------------- """
    @staticmethod
    def get_sample_file_name(rate, bits, seed, extension=".csv"):
        return "{}_{}_{}{}".format(rate, bits, seed, extension)


    def get_all_hit_rate_error_files(self, sample_set_name: str):
        if self._workload_set_name == "cp":
            workload_list = get_all_cp_workloads()
        else:
            raise ValueError("Workload set {} not supported!".format(self._workload_set_name))
        
        hit_rate_error_file_list = []
        for workload_name in workload_list:
            hit_rate_error_data_dir = self.get_hit_rate_error_data_dir_path(sample_set_name, workload_name)
            for hit_rate_error_file in hit_rate_error_data_dir.iterdir():
                hit_rate_error_file_list.append(hit_rate_error_file)
        return hit_rate_error_file_list


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
    

    def get_cum_hit_rate_error_data_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in workload_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._cum_hr_error_data_dir_path.joinpath(workload_name)
    

    def get_sample_cache_features_dir_path(
            self,
            sample_set_name: str,
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._cache_features_dir_path.joinpath(workload_name)
    

    def get_sample_block_error_dir_path(
            self,
            sample_set_name: str,
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._block_error_dir_path.joinpath(workload_name)
    

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
    

    def get_sample_rd_hist_file_path(
            self, 
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        return self.get_sample_rd_hist_dir_path(sample_set_name, workload_name).joinpath("{}_{}_{}.csv".format(rate, bits, seed))
    

    def get_sample_block_error_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_dir_path = self.get_sample_block_error_dir_path(sample_set_name, workload_name)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_all_sample_error_file_path(
            self,
            sample_set_name: str
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)

        sample_error_file_list = []
        for workload_dir in new_base_config._block_error_dir_path.iterdir():
            for output_file_path in workload_dir.iterdir():
                sample_error_file_list.append(output_file_path)
        return sample_error_file_list


    
    def get_sample_feature_file_path(
            self,
            sample_set_name: str, 
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name and "--" not in self._workload_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return self._sample_feature_output_dir_path.joinpath("{}.csv".format(workload_name))




    

    def get_sample_cache_trace_dir_path(
            self,
            sample_set_name: str,
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._cache_trace_dir_path.joinpath(workload_name)


    def get_sample_cache_trace_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_cache_trace_dir_path = self.get_sample_cache_trace_dir_path(sample_set_name, workload_name)
        return sample_cache_trace_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    


    def get_hit_rate_error_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_dir_path = self.get_hit_rate_error_data_dir_path(sample_set_name, workload_name)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_cum_hit_rate_error_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_dir_path = self.get_cum_hit_rate_error_data_dir_path(sample_set_name, workload_name)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    
    
    @staticmethod
    def get_sample_file_info(sample_file_path: Path) -> tuple:
        sample_file_name_split = sample_file_path.stem.split("_")
        rate, bits, seed = int(sample_file_name_split[0]), int(sample_file_name_split[1]), int(sample_file_name_split[2])
        return rate, bits, seed 
    

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
        return new_base_config._block_trace_dir_path.joinpath(workload_name, self.get_sample_file_name(rate, bits, seed))
    

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
    

    """ --------------------------------------------- Post process helpers ---------------------------------------------------------- """


    def get_sample_access_dir_path(
            self,
            sample_set_name: str,
            workload_name: str 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._access_features_dir_path.joinpath(workload_name)
    

    def get_sample_post_process_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_algo_output_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name)


    def get_all_sample_post_process_dir_path(
            self,
            sample_set_name: str,
            metric_name: str,
            algo_bits: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        dir_path = new_base_config._post_process_algo_output_dir_path.joinpath("{}_{}".format(metric_name, algo_bits))
        return list(dir_path.iterdir())
    


    def get_sample_access_feature_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_dir_path = self.get_sample_access_dir_path(sample_set_name, workload_name)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_sample_post_process_output_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        sample_dir_path = self.get_sample_post_process_dir_path(sample_set_name, workload_name, metric_name, algo_bits)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_all_sample_post_process_output_file_path(
            self,
            sample_set_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        output_dir_list = self.get_all_sample_post_process_dir_path(sample_set_name, metric_name, algo_bits)

        output_file_list = []
        for workload_dir in output_dir_list:
            for output_file_path in workload_dir.iterdir():
                cur_rate, cur_bits, cur_seed = self.get_sample_file_info(output_file_path)
                if cur_rate != rate or cur_bits != bits or cur_seed != seed:
                    continue 

                output_file_list.append(output_file_path)
        
        return output_file_list 
                


    def get_post_process_cache_trace_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int,
            num_iter: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_cache_trace_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name, str(num_iter))
    

    def get_post_process_rd_trace_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int,
            num_iter: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_rd_trace_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name, str(num_iter))
    

    def get_post_process_rd_hist_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int,
            num_iter: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_rd_hist_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name, str(num_iter))
    

    def get_post_process_hit_rate_error_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int,
            num_iter: int 
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_hit_rate_error_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name, str(num_iter))
    

    def get_post_process_workload_hit_rate_error_dir_path(
            self,
            sample_set_name: str,
            workload_name: str,
            metric_name: str,
            algo_bits: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_hit_rate_error_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), workload_name)
    

    def get_post_process_cache_trace_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_post_process_cache_trace_dir_path(sample_set_name, workload_name, metric_name, algo_bits, num_iter)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_post_process_rd_trace_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_post_process_rd_trace_dir_path(sample_set_name, workload_name, metric_name, algo_bits, num_iter)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_post_process_rd_hist_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_post_process_rd_hist_dir_path(sample_set_name, workload_name, metric_name, algo_bits, num_iter)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))


    def get_pp_cache_trace_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_cache_trace_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), 
                                                                              workload_name, 
                                                                              self.get_sample_file_name(rate, bits, seed, extension=''))
    

    def get_pp_rd_trace_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_rd_trace_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), 
                                                                              workload_name, 
                                                                              self.get_sample_file_name(rate, bits, seed, extension=''))
    

    def get_pp_rd_hist_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_rd_hist_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), 
                                                                              workload_name, 
                                                                              self.get_sample_file_name(rate, bits, seed, extension=''))


    def get_pp_cache_trace_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_pp_cache_trace_dir_path(sample_set_name, workload_name, metric_name, algo_bits, rate, bits, seed)
        return sample_dir_path.joinpath("{}.csv".format(num_iter))
    

    def get_pp_rd_trace_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_pp_rd_trace_dir_path(sample_set_name, workload_name, metric_name, algo_bits, rate, bits, seed)
        return sample_dir_path.joinpath("{}.csv".format(num_iter))
    

    def get_pp_rd_hist_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_pp_rd_hist_dir_path(sample_set_name, workload_name, metric_name, algo_bits, rate, bits, seed)
        return sample_dir_path.joinpath("{}.csv".format(num_iter))
    

    def get_pp_hit_rate_error_dir_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> Path:
        compound_workload_set_name = self.get_compound_workload_set_name(sample_set_name)
        assert "--" not in sample_set_name,\
            "Sample set or workload name cannot have the string with two dashes --. Current compound workload set name: {}".format(compound_workload_set_name)
        new_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
        return new_base_config._post_process_hit_rate_error_dir_path.joinpath("{}_{}".format(metric_name, algo_bits), 
                                                                              workload_name, 
                                                                              self.get_sample_file_name(rate, bits, seed, extension=''))


    def get_pp_hit_rate_error_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_pp_hit_rate_error_dir_path(sample_set_name, workload_name, metric_name, algo_bits, rate, bits, seed)
        return sample_dir_path.joinpath("{}.csv".format(num_iter))


    def get_post_process_hit_rate_error_file_path(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int,
            num_iter: int 
    ) -> Path:
        sample_dir_path = self.get_post_process_hit_rate_error_dir_path(sample_set_name, workload_name, metric_name, algo_bits, num_iter)
        return sample_dir_path.joinpath(self.get_sample_file_name(rate, bits, seed))
    

    def get_all_post_process_hit_rate_error_files(
            self,
            sample_set_name: str, 
            workload_name: str, 
            metric_name: str, 
            algo_bits: int,
            rate: int, 
            bits: int, 
            seed: int
    ) -> list:
        data_dir = self.get_post_process_workload_hit_rate_error_dir_path(sample_set_name,
                                                                            workload_name,
                                                                            metric_name,
                                                                            algo_bits)
        hit_rate_error_file_list = []
        for num_iter_data_dir in data_dir.iterdir():
            assert num_iter_data_dir.is_dir(), "There is a file in the directory {}.".format(num_iter_data_dir)
            for hit_rate_error_file_path in num_iter_data_dir.iterdir():
                cur_rate, cur_bits, cur_seed = self.get_sample_file_info(hit_rate_error_file_path)

                if rate == cur_rate and bits == cur_bits and seed == cur_seed:
                    hit_rate_error_file_list.append(hit_rate_error_file_path)
        
        return hit_rate_error_file_list


    def get_all_pp_hit_rate_error_files(
                self,
                sample_set_name: str, 
                workload_name: str, 
                metric_name: str, 
                algo_bits: int,
                rate: int, 
                bits: int, 
                seed: int,
                max_num_iter: int = 0 
        ) -> list:
            data_dir = self.get_pp_hit_rate_error_dir_path(sample_set_name,
                                                            workload_name,
                                                            metric_name,
                                                            algo_bits,
                                                            rate,
                                                            bits,
                                                            seed)
            path_list = []
            for data_path in data_dir.iterdir():
                num_iter = int(data_path.stem)
                if max_num_iter == 0:
                    path_list.append(data_path)
                elif num_iter <= max_num_iter:
                    path_list.append(data_path) 
            return path_list
    
    @staticmethod
    def get_all_cp_workloads():
        return ["w{}".format(i) if i > 9 else "w0{}".format(i) for i in range(1, 107)]

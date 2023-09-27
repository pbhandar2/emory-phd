"""This class contains all configuration related to post processing samples."""

from pathlib import Path 

from keyuri.config.Config import GlobalConfig


class PostProcessConfig:
    def __init__(
            self,
            global_config: GlobalConfig = GlobalConfig()
    ) -> None:
        self._global_config = global_config 
        self._source_dir_path = self._global_config.source_dir_path
        self._metadata_dir_path = self._global_config.metadata_dir_path

        # block trace after post-processing 
        self._block_trace_dir_path = self._source_dir_path.joinpath("postprocess_sample_block_traces")

        # error per iteration during post-processing 
        self._per_iteration_data_dir_path = self._metadata_dir_path.joinpath("postprocess", "per_iteration_error")

        # overall statistics of post-processing 
        self._postprocess_stat_dir_path = self._metadata_dir_path.joinpath("postprocess", "overall")
    

    def get_block_trace_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int
    ) -> Path:
        data_dir = self._block_trace_dir_path
        return data_dir.joinpath(
            postprocess_type,
            sample_type,
            workload_type,
            workload_name,
            "{}_{}_{}.csv".format(rate, bits, seed)
        )
    

    def get_metadata_file_path(
            self,
            postprocess_type: str, 
            sample_type: str,
            workload_type: str,
            workload_name: str,
            rate: int,
            bits: int,
            seed: int
    ) -> Path:
        data_dir = self._postprocess_stat_dir_path
        return data_dir.joinpath(
            postprocess_type,
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
            seed: int
    ) -> Path:
        data_dir = self._per_iteration_data_dir_path
        return data_dir.joinpath(
            postprocess_type,
            sample_type,
            workload_type,
            workload_name,
            "{}_{}_{}.csv".format(rate, bits, seed)
        )
    

    def get_post_process_params_from_file_path(
            self,
            post_process_file_path: Path 
    ) -> dict:
        post_process_file_name = post_process_file_path.stem 
        rate, bits, seed = post_process_file_name.split("_")
        workload_name = post_process_file_path.parent.name 
        workload_type = post_process_file_path.parent.parent.name 
        sample_type = post_process_file_path.parent.parent.parent.name 
        post_process_type = post_process_file_path.parent.parent.parent.parent.name 
        return {
            "rate": rate, 
            "seed": seed, 
            "bits": bits, 
            "workload_name": workload_name,
            "workload_type": workload_type, 
            "sample_type": sample_type,
            "post_process_type": post_process_type
        }
from pathlib import Path 
from json import dump, JSONEncoder
from numpy import integer, floating, ndarray

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.RDHistogram import RDHistogram


class NpEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        if isinstance(obj, floating):
            return float(obj)
        if isinstance(obj, ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)



class CompileHrErr:
    def __init__(self) -> None:
        self._dir_config = BaseConfig()


    def get_output_path(self, workload, rate, bits, seed):
        return self._output_dir.joinpath(workload, "{}_{}_{}.json".format(int(100*rate), bits, seed))


    def create_all(self, sample_set_name: str):
        print("Create HR error for all samples in set {}".format(sample_set_name))
        # Make sure for each sample rd hist, there is its subsequent error file 
        for workload_name in self._dir_config.get_all_cp_workloads():
            print("Evaluating workload {}".format(workload_name))
            full_rd_hist_file = self._dir_config.get_rd_hist_file_path(workload_name)
            if not full_rd_hist_file.exists():
                continue 
            
            for sample_rd_hist_file in self._dir_config.get_sample_rd_hist_dir_path(sample_set_name, workload_name).iterdir():
                print("Evaluating sample rd hist {}.".format(sample_rd_hist_file))
                if sample_rd_hist_file.stat().st_size == 0:
                    print("File {} has size 0.".format(sample_rd_hist_file))
                    continue 
                
                output_dir = self._dir_config.get_cum_hit_rate_error_data_dir_path(sample_set_name, workload_name)
                output_dir.mkdir(exist_ok=True, parents=True)
                output_path = output_dir.joinpath("{}.json".format(sample_rd_hist_file.stem))
                if output_path.exists():
                    print("{} exists with size {}.".format(output_path, output_path.stat().st_size))
                    continue 
                
                output_path.touch()
                print("Created file: ", output_path)
                sampling_rate, _, _ = self._dir_config.get_sample_file_info(sample_rd_hist_file)

                err_dict = self.compute_sample_hr_err(full_rd_hist_file, sample_rd_hist_file, sampling_rate/100.0)
                print("FINAL")
                print(err_dict)
                with output_path.open("w") as output_handle:
                    dump(err_dict, output_handle, indent=2)
                    #dump(output_handle, err_dict, cls=NpEncoder)
                print("Done: ", output_path)


    @staticmethod
    def compute_sample_hr_err(full_rd_hist_file: Path, sample_rd_hist_file: Path, sampling_ratio: float) -> dict:
        full_rd_hist = RDHistogram(-1)
        full_rd_hist.load_rd_hist_file(full_rd_hist_file)

        sample_rd_hist = RDHistogram(-1)
        sample_rd_hist.load_rd_hist_file(sample_rd_hist_file)

        return full_rd_hist.get_sample_absolute_hit_ratio_err(sample_rd_hist, sampling_ratio)
from pathlib import Path 
from keyuri.experiments.HitRateError import CumHitRateError


def main():
    hit_rate_err_file = Path.home().joinpath("data/meta/cum_sample_hr_err/hr_err.csv")
    hit_rate_err_file.parent.mkdir(parents=True, exist_ok=True)
    cum_hit_rate_error = CumHitRateError("basic", hit_rate_err_file)
    cum_hit_rate_error.generate_all()


if __name__ == "__main__":
    main()
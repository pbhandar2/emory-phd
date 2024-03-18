from pathlib import Path 
import logging
logging.basicConfig(level=logging.INFO)

from keyuri.analysis.CompileHrErr import CompileHrErr


def main():
    compile_hr_err = CompileHrErr()
    compile_hr_err.create_all("basic")


if __name__ == "__main__":
    main()
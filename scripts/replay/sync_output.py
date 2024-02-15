from argparse import ArgumentParser 
from pathlib import Path 
from os import environ 

from cydonia.util.S3Client import S3Client


OUTPUT_DIR = Path("/research2/mtc/cp_traces/pranav-phd/replay_output")
S3_KEY_DIR = "mtcachedata/new-replay/output/"


def main():
    s3_client = S3Client(environ["AWS_KEY"], environ["AWS_SECRET"], environ["AWS_BUCKET"])
    s3_client.sync_s3_prefix_with_local_dir(S3_KEY_DIR, OUTPUT_DIR)


if __name__ == "__main__":
    main()
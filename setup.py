from setuptools import setup

setup (
    name="keyuri",
    version="0.1",
    packages=["keyuri.replaydb", "keyuri.config", "keyuri.experiments"],
    install_requires=["numpy", "pandas", "argparse", "pathlib"]
)
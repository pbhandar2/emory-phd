from setuptools import setup

setup (
    name="keyuri",
    version="0.1",
    packages=["keyuri.config", "keyuri.experiments", "keyuri.analysis", "keyuri.tracker", "keyuri.dataManager"],
    install_requires=["numpy", "pandas", "argparse", "pathlib"]
)
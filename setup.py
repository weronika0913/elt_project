from setuptools import setup, find_packages

setup(
    name="elt_pipeline",
    version="0.1",
    packages=find_packages('scripts'),
    package_dir={'': 'scripts'}
)

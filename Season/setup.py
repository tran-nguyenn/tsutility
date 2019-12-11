import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="example-pkg-kjchoi10", # Replace with your own username
    version="0.0.1",
    author="Kevin Choi",
    author_email="kjchoi10@gmail.com",
    description="time series calculations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/newelldatascience/seasonal_autocorrelation/tree/master/Season",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)# -*- coding: utf-8 -*-


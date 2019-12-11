import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tsDataScience", # Replace with your own username
    version="0.0.1",
    author="Kevin Choi",
    author_email="kjchoi10@gmail.com",
    description="time series calculations",
    long_description=long_description,
    long_description_content_type="calculation",
    url="https://github.com/newelldatascience/seasonal_autocorrelation/tree/master/tsDataScience",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.0',
)# -*- coding: utf-8 -*-


""" A setuptools based setup module.
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the version information
about = {}
ver_path = path.join(here, "nemwriter", "version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), about)

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="nemwriter",
    version=about["__version__"],
    description="Write meter readings to AEMO NEM12 and NEM13 data files",
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Alex Guinman",
    author_email="alex@guinman.id.au",
    keywords=["energy", "NEM12", "NEM13"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[],
    license="MIT",
    url="https://github.com/aguinane/nem-writer",
    project_urls={"Bug Reports": "https://github.com/aguinane/nem-writer/issues"},
)

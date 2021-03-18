from setuptools import setup, find_packages
from os import path
from io import open

here = path.abspath(path.dirname(__file__))


setup(
    name="rss",
    version="0.0.1",  # Required
    description="real simple segy",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Intended Audience :: Science/Research",
        "License :: OSI Approved ::  BSD-3-Clause License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(exclude=["contrib",
                                    "docs",
                                    "tests"]),
    python_requires=">=3.6",

    # This is for getting through appveyor, install tensorflow_gpu if you can.
    install_requires=["numpy==1.19.5", "ebcdic", "ibm2ieee", "tqdm",
                      "notebook", 'matplotlib==3.3.3', 'numcodecs', 's3fs==0.5.1',
                      "scipy==1.5.4",
                      "zarr", 'aiohttp==3.7.2'],  
    # List additional groups of dependencies here (e.g. development
    # dependencies). Users will be able to install these using the "extras"
    # syntax, for example:
    #
    #   $ pip install sampleproject[dev]
    #
    # Similar to `install_requires` above, these must be valid existing
    # projects.
    extras_require={},
    # If there are data files included in your packages that need to be
    # installed, specify them here.
    #
    # If using Python 2.6 or earlier, then these have to be included in
    # MANIFEST.in as well.
    package_data={},
    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # `pip` to create the appropriate form of executable for the target
    # platform.
    entry_points={},
    # List additional URLs that are relevant to your project as a dict.
    project_urls={},
)

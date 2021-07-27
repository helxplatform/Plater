import os
import re
import sys
from shutil import rmtree
from setuptools import setup, find_packages, Command

current = os.path.abspath(os.path.dirname(__file__))


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

version = "1.9.6"

class PublishClass(Command):
    description = "Publish the package"
    user_options = []

    # This method must be implemented
    def initialize_options(self):
        pass

    # This method must be implemented
    def finalize_options(self):
        pass

    def run(self):
        try:
            print(f"-----> removing previous builds")
            rmtree(os.path.join(current, 'dist'))
            rmtree(os.path.join(current, 'build'))
        except Exception as e:
            print(f"-----> Exception: {e}")
            pass
        os.system('python setup.py sdist bdist_wheel --universal')
        # os.system('twine upload --repository dist/*')
        sys.exit()

setup(
    name="PLATER-GRAPH",
    version=version,
    maintainer="Renci",
    maintainer_email="yaphetkg@renci.org",
    description="Graph DB interface for Translator API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yaphetkg/plater.git",
    # package_dir={"": 'PLATER'},
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "coverage",
        "pyaml==20.3.1",
        "pytest==5.4.1",
        "pytest-asyncio==0.14.0",
        "starlette==0.13.6",
        "uvicorn==0.11.7",
        "httpx",
        "redis"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    dependency_links= [
        "git+git://github.com/patrickkwang/biolink-model-toolkit@master#egg=bmt",
        "git+https://github.com/ranking-agent/reasoner.git",
        "git+https://github.com/TranslatorSRI/reasoner-pydantic@v1.0#egg=reasoner-pydantic",
        "git+https://github.com/patrickkwang/fastapi#egg=fastapi",
        "git+https://github.com/redislabs/redisgraph-py.git"
    ],
    python_requires='>=3.7',
    cmdclass={
        'publish': PublishClass,
    },
)
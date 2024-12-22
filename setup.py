from setuptools import setup, find_packages

setup(
    name="chicago-dig-bot",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'requests',
    ]
)

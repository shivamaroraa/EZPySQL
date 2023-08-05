from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="EZPySQL",
    version="0.1.0",
    author="Shivam Arora",
    author_email="shivam.arora98@outlook.com",
    description="A Python module for interacting with SQL databases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shivamaroraa/EZPySQL",
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'sqlite3',
        'pymysql'
    ],
)

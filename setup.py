"""Setup configuration for data transformation tool."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="data-transformation-tool",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A YAML-to-SQL data transformation tool for medallion architecture",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": ["pytest>=7.0", "black>=22.0", "flake8>=4.0", "mypy>=0.991"],
        "docs": ["sphinx>=4.0", "sphinx-rtd-theme>=1.0"],
    },
    entry_points={
        "console_scripts": [
            "data-transform=data_transformation_tool.cli:main",
        ],
    },
)

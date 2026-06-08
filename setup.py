from setuptools import setup, find_packages

setup(
    name="sicred-etl",
    version="1.0.0",
    description="ETL Pipeline - Sicred Asset Management",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    install_requires=[
        "pyspark==3.5.1",
        "PyYAML==6.0.1",
        "python-dotenv==1.0.1",
    ],
    extras_require={
        "test": [
            "pytest==8.1.1",
            "pytest-cov==5.0.0",
            "pytest-mock==3.14.0",
        ]
    },
)
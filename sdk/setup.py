"""Setup configuration for data-lineage-hub-sdk."""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="data-lineage-hub-sdk",
    version="1.0.0",
    author="Data Platform Team",
    author_email="data-platform@company.com",
    description="Python SDK for Data Lineage Hub - OpenLineage and OpenTelemetry integration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/company/data-lineage-hub-sdk",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.9",
    install_requires=[
        "httpx>=0.24.0",
        "pydantic>=2.0.0",
        "structlog>=23.0.0",
        "openlineage-python>=1.0.0",
        "opentelemetry-api>=1.20.0",
        "opentelemetry-sdk>=1.20.0",
        "opentelemetry-instrumentation>=0.41b0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-mock>=3.10.0",
            "ruff>=0.1.0",
            "mypy>=1.5.0",
            "coverage>=7.0.0",
        ],
        "all": [
            "opentelemetry-instrumentation-requests",
            "opentelemetry-instrumentation-urllib3",
            "opentelemetry-instrumentation-httpx",
            "opentelemetry-exporter-otlp",
        ],
    },
    entry_points={
        "console_scripts": [
            "lineage-hub=data_lineage_hub_sdk.cli:main",
        ],
    },
)
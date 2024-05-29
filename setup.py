from setuptools import find_packages, setup

setup(
    name="dagster_great_expectations",
    packages=find_packages(exclude=["dagster_great_expectations_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "great_expectations",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

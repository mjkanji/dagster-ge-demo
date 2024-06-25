from setuptools import find_packages, setup

setup(
    name="dagster_ge_demo",
    packages=find_packages(exclude=["dagster_ge_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-pandas",
        "great_expectations",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff", "ipykernel"]},
)

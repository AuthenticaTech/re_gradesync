from setuptools import find_packages, setup

setup(
    name="re_gradesync",
    packages=find_packages(exclude=["re_gradesync_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

from setuptools import (
    setup,
    find_packages,
)

setup(
    name = "redisxchange",
    url = "https://github.com/nicklasxyz/redisxchange",
    author = "Nicklas Sindlev Andersen",
    packages = find_packages(include=["redisxchange"]),
    include_package_data = True,
    install_requires = ["msgpack", "redis-py", "asgiref"],
    version = "0.1",
    license = "MIT",
    description = "",
    python_requires=">=3.7",
    long_description = open("README.md").read(),
)
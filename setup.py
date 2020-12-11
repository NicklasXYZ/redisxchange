from setuptools import setup

setup(
    name = "redisxchange",
    url = "https://github.com/nicklasxyz/redisxchange",
    author = "Nicklas Sindlev Andersen",
    packages = ["redisxchange"],
    include_package_data = True,
    install_requires = ["msgpack", "redis-py"],
    version = "0.1",
    license = "MIT",
    description = "",
    python_requires=">=3.8",
    long_description = open("README.md").read(),
)
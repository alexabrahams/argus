#
# Copyright (C) 2015-2022 Man Group Ltd
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
# USA

import logging
import sys

from setuptools import find_packages
from setuptools import setup
from setuptools.command.test import test as TestCommand

long_description_content_type = "text/markdown"
long_description = open("README.md").read()
changelog = open("CHANGES.md").read()


class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level="DEBUG")

        # import here, cause outside the eggs aren't loaded
        import pytest
        import six

        args = [self.pytest_args] if isinstance(self.pytest_args, six.string_types) else list(self.pytest_args)
        args.extend(
            [
                "--cov",
                "argus",
                "--cov-report",
                "xml",
                "--cov-report",
                "html",
                "--junitxml",
                "test-results/junit.xml",
            ]
        )
        errno = pytest.main(args)
        sys.exit(errno)


setup(
    name="argus",
    version="1.0",
    author="Alex Abrahams",
    description=("Tick DB"),
    packages=find_packages(exclude=["tests", "tests.*", "benchmarks"]),
    cmdclass={"test": PyTest},
    setup_requires=[
        "six",
        "numpy",
    ],
    install_requires=[
        "decorator",
        "enum-compat",
        "mock",
        "mockextras",
        "pandas",
        "numpy",
        "pymongo",
        "pytz",
        "tzlocal",
        "lz4",
    ],
    tests_require=["mock", "mockextras", "pytest", "pytest-cov", "pytest-timeout", "pytest-xdist", "lz4"],
    entry_points={
        "console_scripts": [
            "argus_init_library = argus.scripts.argus_init_library:main",
            "argus_list_libraries = argus.scripts.argus_list_libraries:main",
            "argus_delete_library = argus.scripts.argus_delete_library:main",
            "argus_enable_sharding = argus.scripts.argus_enable_sharding:main",
            "argus_copy_data = argus.scripts.argus_copy_data:main",
            "argus_create_user = argus.scripts.argus_create_user:main",
            "argus_prune_versions = argus.scripts.argus_prune_versions:main",
            "argus_fsck = argus.scripts.argus_fsck:main",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Operating System :: POSIX",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries",
    ],
)

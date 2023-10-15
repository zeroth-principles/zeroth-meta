# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 Zeroth-Principles
#
# This file is part of Zeroth-Quant.
#
#  Zeroth-Quant is free software: you can redistribute it and/or modify it under the
#  terms of the GNU General Public License as published by the Free Software
#  Foundation, either version 3 of the License, or (at your option) any later
#  version.
#
#  Zeroth-Quant is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
#  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License along with
#  Zeroth-Quant. If not, see <http://www.gnu.org/licenses/>.f

from setuptools import setup, find_packages
import os

setup(
    name="zeroth-meta",
    version="1.0.0.101",
    description="Base Repo",
    author = "Zeroth-Principles-Engineering",
    author_email = "engineering@zeroth-principles.com",
    license="GPLv3",
    url="https://zeroth-principles.com",
    # python_requires=">=3.5.0",
    # packages = find_packages(where="zeroth-meta"),
    packages = find_packages(),
    # package_dir={'': "zeroth-meta"},
    install_requires=[],
    extras_require={},


)
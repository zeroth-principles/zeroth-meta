# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 Zeroth-Principles
#
# This file is part of Zeroth-Meta.
#
#  Zeroth-Meta is free software: you can redistribute it and/or modify it under the
#  terms of the GNU General Public License as published by the Free Software
#  Foundation, either version 3 of the License, or (at your option) any later
#  version.
#
#  Zeroth-Meta is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
#  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License along with
#  Zeroth-Meta. If not, see <http://www.gnu.org/licenses/>.
#

import sys
sys.path.append("C:\\Users\\raman\\zeroth\\zeroth-meta\\")
from metaclasses.metaclasses import MultitonMeta


class MyClass(metaclass=MultitonMeta):
    def __init__(self, params): # params is a dictionary
        super(MyClass, self).__init__()
        self.params = params

    def __str__(self):
        return f"MyClass({self.params})"

m1 = MyClass(params={"a": 1, "b": 2})
m2 = MyClass(params={"b": 2, "a": 1})

# generate comparison
print(m1 == m2)

# generate compreshensive tests for classes in src/metaclasses.py






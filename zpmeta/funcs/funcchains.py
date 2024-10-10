# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 Zeroth-Principles.
# 
# Created on 6/30/2024 by Zeroth-Principles-Engineering.
# For suggestions, please write to engineering@zeroth-principles.com.
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

__copyright__ = '2024 Zeroth Principles'
__license__ = 'GPLv3'
__docformat__ = 'google'
__author__ = 'Zeroth Principles Engineering'
__email__ = 'engineering@zeroth-principles.com'
__authors__ = []

""" * """

class Chain:
    def __init__(self, funcs=None) -> None:
        self.funcs = funcs

    def __call__(self, operand=None, params: dict = None) -> object:
        result = operand
        # reverse traverse the funcs
        for func, param in reversed(zip(self.funcs, params)):
            result = func(result, param)

        return result


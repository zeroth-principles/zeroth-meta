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

"""Superclasses for frequently used design patterns to iterate over data, parameters, and functions."""

__copyright__ = '2023 Zeroth Principles'
__license__ = 'GPLv3'
__docformat__ = 'google'
__author__ = 'Zeroth Principles Engineering'
__email__ = 'engineering@zeroth-principles.com'

import logging
from zpmeta.utils.common_utils import deep_update


class MapFuncs:
    def __init__(self, func, params=None) -> None:
        self.func = func
        self.params = params

    def __call__(self, operand=None, params: dict = None) -> object:
        if params is not None:
            params = deep_update(self.params, params)
        else:
            params = self.params.copy()

        results = {}
        for key, func in self.func.items():
            results[key] = func(operand, params)

        return results


class MapParams:
    def __init__(self, func, params=None) -> None:
        self.func = func
        self.params = params

    def __call__(self, operand=None, params: dict = None) -> object:
        if params is not None:
            params = deep_update(self.params, params)
        else:
            params = self.params.copy()

        results = {}
        for key, sub_params in params.items():
            results[key] = self.func(operand, sub_params)

        return results


class MapOperands:
    def __init__(self, func, params=None) -> None:
        self.func = func
        self.params = params

    def __call__(self, operand=None, params: dict = None) -> object:
        if params is not None:
            params = deep_update(self.params, params)
        else:
            params = self.params.copy()

        results = {}
        for key, sub_operand in operand.items():
            results[key] = self.func(sub_operand, params)

        return results


class Funcify:
    def __init__(self, target_callable, operand_key, default_params=None) -> None:
        self.target_callable = target_callable
        self.operand_key = operand_key
        self.default_params = default_params

    def __call__(self, operand=None, params: dict = None) -> object:
        if params is not None:
            params = deep_update(self.default_params, params)
        else:
            params = self.default_params.copy()
        
        if self.operand_key is not None:
            params[self.operand_key] = operand
        
        results = self.target_callable(**params)

        return results
    
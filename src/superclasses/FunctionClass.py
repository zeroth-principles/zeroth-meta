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
"""Superclasses for frequently used design patterns."""


class FunctionClass(object):
    """
    Class that behaves like functions. It is used to impose a structure on data data processing to facilitate 
    code modularity and reusability.
    """

    def __init__(self, params: dict = None, pre=None) -> None:
        if params is None or isinstance(params, str):
            self.params = self.std_params(params)
        elif isinstance(params, tuple):
            base_params = self.std_params(params[0])
            self.params = base_params.update(params[1], overwrite=True, append=True)
        elif isinstance(params, dict):
            base_params = self.std_params()
            self.params = base_params.update(params, overwrite=True, append=True)
        else:
            raise TypeError("params must be a str, tuple, or a dict!")

        self.pre = pre

    @classmethod
    def std_params(cls, name: str = None) -> dict:
        return dict()

    @classmethod
    def pkeys(cls) -> list:
        return []

    def __call__(self, operand=None, period: tuple = None, params: dict = None) -> object:
        results = self._run(operand, period, params)
        return results

    def _run(self, operand=None, period: tuple = None, params: dict = None) -> object:
        params = self.params.update(params, overwrite=True, append=True)

        if self.pre is not None:
            if isinstance(self.pre, type):
                operand = self.pre()(operand, period, params)
            else:
                operand = self.pre(operand, period)
        results = self.wrapped_execute(operand, period, params)
        return results

    def wrapped_execute(self, operand=None, period: tuple = None, params: dict = None) -> object:
        results = self.execute(operand, period, params)
        return results

    @classmethod
    @abstractmethod
    def execute(cls, operand=None, period: tuple = None, params: dict = None) -> object:
        return operand

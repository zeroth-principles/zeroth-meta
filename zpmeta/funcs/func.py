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

__copyright__ = '2023 Zeroth Principles Research'
__license__ = 'GPLv3'
__docformat__ = 'google'
__author__ = 'Zeroth Principles Engineering'
__email__ = 'engineering@zeroth-principles.com'

import logging
import abc
from zpmeta.utils.common_utils import deep_update


class Func(metaclass=abc.ABCMeta):
    """Callable class  used to impose a structure on data processing

    Raises:
        TypeError: _description_

    Returns:
        results: Results of the function
    """    

    def __init__(self, params: dict = None, xfunc=None) -> None:
        if params is None or isinstance(params, str):
            self.params = self._std_params(params)
        elif isinstance(params, tuple):
            self.params = self._std_params(params[0])
            self.params = deep_update(self.params, params)
        elif isinstance(params, dict):
            self.params = self._std_params()
            self.params = deep_update(self.params, params)
        else:
            raise TypeError("params must be a str, tuple, or a dict!")
        
        logging.info("INIT %s %s", self.__class__.__name__, self.params)

        self.xfunc = xfunc

    @classmethod
    def _std_params(cls, name: str = None) -> dict:
        return {}

    def __call__(self, operand=None, params: dict = None) -> object:
        if params is not None:
            params = deep_update(self.params, params)
        else:
            params = self.params
        
        if callable(self.xfunc):
            operand = self.xfunc(operand)
        
        results = self._execute(operand, params)        
        return results

    @staticmethod
    def check_consistency(operand=None, params: dict = None) -> object:
        pass

    @classmethod
    @abc.abstractmethod
    def _execute(cls, operand=None, params: dict = None) -> object:
        pass


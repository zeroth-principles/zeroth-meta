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
"""Metaclasses and Abstract Base Classes for frequently used design patterns."""

__copyright__ = '2023 Zeroth Principles Research'
__license__ = 'GPLv3'
__docformat__ = 'google'
__author__ = 'Zeroth Principles Engineering'
__email__ = 'engineering@zeroth-principles.com'
__authors__ = ['Ramanuj Lal <ramanujlal@zeroth-principles.com>']

import json
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Tuple, Optional
import logging
from zpmeta.utils.common_utils import custom_serializer

class IsolatedMeta(ABCMeta):
    """Metaclass for isolated classes.

    Isolated classes create a new subclass for any instance to allow the
    modification of class methods without side effects. Common use cases for
    isolated classes include Singletons, Multitons and built classes, that avoid
    generic programming in favor of higher efficiency or lower memory usage.

    """
    def __call__(cls, *args: Any, **kwds: Any) -> object:
        # Create new subclass of the given class. Set the attribute '__slots__'
        # to an empty list, to allow the usage of slots.
        subcls = IsolatedMeta(cls.__name__, (cls, ), {'__slots__': []})

        # Create an instance of the new subclass. Note, that if the class does
        # not implement an __init__ method a TypeError is raised. In this case
        # the class is called without arguments.
        try:
            return super(IsolatedMeta, subcls).__call__(*args, **kwds)
        except TypeError as err:
            if 'takes no arguments' in str(err):
                return super(IsolatedMeta, subcls).__call__()
            raise


class SingletonMeta(IsolatedMeta):
    """Metaclass for Singletons.

    Singleton classes only create a single instance per application and
    therefore by definition are special case of isolated classes. This creation
    pattern ensures the application global uniqueness and accessibility of
    instances, comparably to global variables. Common use cases comprise
    logging, sentinel objects and application global constants (given as
    immutable objects).

    """
    _instance: Optional[object] = None

    def __call__(cls, *args: Any, **kwds: Any) -> object:
        if not cls._instance:

            # Create an instance of the class. Note, that if the class does not
            # implement an __init__ method a TypeError is raised. In this case
            # the class is called without arguments.
            try:
                obj = super(SingletonMeta, cls).__call__(*args, **kwds)
            except TypeError as err:
                if 'takes no arguments' in str(err):
                    obj = super(SingletonMeta, cls).__call__()
                else:
                    raise
            cls._instance = obj

        return cls._instance


class MultitonMeta(IsolatedMeta):
    """Metaclass for Multitons.

    Multiton Classes only create a single instance per given arguments by using
    a new subclass for class isolation. This allows a controlled creation of
    multiple distinct objects, that are globally accessible and unique. Multiton
    classes may be regarded as a generalization of Singletons in the sense of
    'Collections of Singletons'. Common use cases comprise application global
    configurations, caching and collections of constants (given as immutable
    objects).

    """
    _registry: Dict[Tuple[type, Any], object] = {}

    def __call__(cls, *args: Any, **kwds: Any) -> object:
        # Create 'fingerprint' of instance. Beware: The fingerprint is only 
        # hashable if all given arguments and keywords are hashable. Therupon
        # Check registry for the fingerprint. If the fingerprint is not hashable
        # create and return and an instance of the class. If the the fingerprint
        # could not not be found in the registry, create a class instance, add
        # it to the registry and return the instance.
        # TODO: Add a warning if the fingerprint is not hashable.
        
        logging.info("args: %s ; kwds: %s", args, kwds)
        if len(args) > 0:
            key = (cls, json.dumps(args[0], default= custom_serializer, sort_keys=True)   )
        else:
            if 'params' in kwds:
                key = (cls, json.dumps(kwds['params'], default= custom_serializer, sort_keys=True))
            else:
                raise KeyError("MultitonMeta requires an argument or a 'params'")
            
        logging.info("Multiton checking registry for key: %s", key)
    
        if key in cls._registry:
            logging.info("Multiton Found Instance of %s %s", *key)
            obj = cls._registry[key]
        else:
            logging.info("Multiton No Instance of %s %s", *key)
            
            # Create new subclass of the given class. Set the attribute '__slots__'
            # to an empty list, to allow the usage of slots.
            subcls = MultitonMeta(cls.__name__, (cls, ), {'__slots__': []})

            # Create an instance of the class. Note, that if the class does not
            # implement an __init__ method a TypeError is raised. In this case the
            # class is called without arguments.
            try:
                obj = super(MultitonMeta, cls).__call__(*args, **kwds)
            except TypeError as err:
                if 'takes no arguments' in str(err):
                    obj = super(MultitonMeta, cls).__call__()
                else:
                    raise

            # Add the instance to the registry.
            logging.info("Multiton Registering Instance of %s %s", *key)
            cls._registry[key] = obj
            
        return obj

Mu = MultitonMeta
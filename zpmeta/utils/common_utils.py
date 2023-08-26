"""common util file contains common functions to support zpmeta"""

__copyright__ = '2023 Zeroth Principles Research'
__license__ = 'GPLv3'
__docformat__ = 'google'
__author__ = 'Zeroth Principles Engineering'
__email__ = 'engineering@zeroth-principles.com'
__authors__ = ['Deepak Singh <deepaksingh@zeroth-principles.com>']


import pandas as pd
import numpy as np
import json

def deep_update(d, u):
    """Deep update of dict d with dict u."""
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def custom_serializer(obj):
    """Custom JSON serializer that converts built-in functions to strings."""
    if callable(obj):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def json_dump(data, **kwargs):
    """Modified json.dumps function to handle built-in functions."""
    return json.dumps(data, default=custom_serializer, **kwargs)
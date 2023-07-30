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

import itertools
from abc import abstractmethod, ABCMeta


class CachedDataPanelSource(object):
    """ Superclass for data generators with parameters.
    
    This class generate panel data (cross-sectional and time-series) given a dict of parameters
    Instances of the class behave like functions.
    It has memory and once called for a list of ids/variables and date range, it does not re-run for that set again 
    when called a second time for the same set of inputs, but only appends data for new inputs.
    ----
    [01 Jul 2023] Created
    ----
    TODO: Add logging
    """
    def __init__(self, params):
        super(CachedDataPanelSource, self).__init__()
        self.params = params
        self.dimensions = dict(xs=True, ts=True)
        self.appendable = dict(xs=False, ts=False)
        self.value = None
        self.entities, self.period = None, None
        # self.logger = DataLogHandler()

    def __repr__(self):
        return super(CachedDataPanelSource, self).__repr__() + str(self.params)

    def __call__(self, entities=None, period=None):
        return self._run(entities, period)

    def __str__(self):
        return self.__repr__()

    # @DataLogHandler().log_level()
    def _run(self, entities=None, period=None):
        print("RUN " + str(self.__class____name__))
        print("DEFINITION: \n", self.params)

        if self.value is None:
            print("EXEC INIT: ", entities, period)
            value, period_exec, items_exec = self.wrapped_execute("initial", items, period)
            self.update(period=value)
            self.period, self.entities = period_exec, items_exec
        else:
            print("EXEC Nth: ", entities, period)
            appendable_ts = self.dimensions.ts is False or (self.dimensions.ts is True and self.appendable.ts is True)
            appendable_xs = self.dimensions.xs is False or (self.dimensions.xs is True and self.appendable.xs is True)
            incremental_period, total_period = self.mismatch_period(period)
            incremental_items, decremental_items, total_items  = self.mismatch_entities(entities)
            print("INCREMENTAL Items: ", incremental_items)
            print("TOTAL Items: ", total_items)
            print("DECREMENTAL Items: ", decremental_items)
            print("INCREMENTAL Period: ", incremental_period)
            print("TOTAL Period: ", total_period)
            print("APPENDABLE TS:%s XS:% " % (appendable_ts, appendable_xs))
            if appendable_ts and appendable_xs:
                if self.dimensions.xs and incremental_items is not None:
                    print("EXEC INCREMENTAL XS: ", self.period, incremental_items)
                    xs_data, _, _ = self.wrapped_execute("ts", incremental_items, self.period)
                    self.entities = total_items
                    self.update(period=xs_data)
                if self.dimensions.ts and incremental_period is not None:
                    print("EXEC INCREMENTAL TS: ", incremental_period, self.entities)
                    ts_data, _, _ = self.wrapped_execute("xs", self.entities, incremental_period)
                    self.period = total_period
                    self.update(entities=ts_data)
            elif appendable_ts and not appendable_xs:
                if incremental_items is None and decremental_items is None:
                    if self.dimensions.ts and incremental_period is not None:
                        print("EXEC INCREMENTAL TS: ", incremental_period, self.entities)
                    ts_data, _, _ = self.wrapped_execute("xs", self.entities, incremental_period)
                    self.period = total_period
                    self.update(entities=ts_data)
            else:
                print("EXEC ALL: ", total_period, total_items)
                self.value, _, _ = self.wrapped_execute("all", total_items, total_period)
                self.period, self.entities = total_period, total_items

        requested_value = self.subset(entities, period)
        print("DONE " + str(self.__class__.__name__))
        return requested_value

    def wrapped_execute(self, call_type=None, entities=None, period=None):
        # with DataLogHandler().log_level():
        results = self.execute(call_type=call_type, entities=entities, period=period)
        return results

    @abstractmethod
    def execute(self, call_type=None, entities=None, period=None):
        pass

    def mismatch_period(self, period):
        if period is None:
            incremental, total = None, self.period

        else:
            if self.period is None:
                incremental, total = period, period
            else:
                total = (min(period[0], self.period[0]), max(period[1], self.period[1]))
            if total[0] < self.period[0]:
                if total[1] > self.period[1]:
                    incremental = (total[0], total[1])
                else:  # total[1] == self.ts[1]
                    incremental = (total[0], self.period[0])
            else:  # total[0]. == self.ts[0]
                if total[1] > self.period[1]:
                    incremental = (self.period[1], total[1])
                else:  # total [1] == self.ts[1]
                    incremental = None

        return incremental, total

    def mismatch_entities(self, entities):
        if self.entities is None:
            incremental_entities, total_entities = entities, entities
        else:
            initial = [dict(zip(self.entities.__dict__, x))
                       for x in itertools.product(*self.entities.__dict__.values())]
            new = [dict(zip(entities.__dict__, x)) for x in itertools.product(*entities.__dict__.values())]
            total_entities = dict()
            for level in self.entities.keys():
                s1, s2 = set(self.entities[level]), set(entities[level])
                total_entities[level] = list(s1.union(s2))
            total = [dict(itertools.izip(total_entities.__dict__, x)) for x in itertools.product(
                *total_entities.__dict__.values())]
            incremental = filter(lambda x: x not in initial, total)
            if len(incremental) > 0:
                incremental_entities = dict()
                for level in self.entities.keys():
                    incremental_entities[level] = list(set(map(lambda x: [level], incremental)))
            else:
                incremental_entities = None
            decremental = filter(lambda x: x not in new, total)
            if len(decremental) > 0:
                decremental_entities = dict()
            else:
                decremental_entities = None

        return incremental_entities, decremental_entities, total_entities

    @abstractmethod
    def update(self, entities=None, period=None):
        pass

    @abstractmethod
    def subset(self, entities=None, period=None):
        pass

    def reset(self):
        self.entities, self.period = None, None
        self.value = None


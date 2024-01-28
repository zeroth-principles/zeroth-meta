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

import logging
import itertools
from abc import abstractmethod, ABCMeta
from pandas import DataFrame, Series, concat, MultiIndex


class PanelSource:
    """ Superclass for cached panel data generation.
    
    This callable class generate panel data (cross-sectional and time-series) given a dict of parameters.
    It has memory and once called for a list of ids/variables and date range, it does not re-run for that set again 
    when called a second time for the same set of inputs, but only appends data for new inputs.
    ----
    [01 Jul 2023] Created
    ----
    TODO: Add logging
    """
    def __init__(self, params: dict = None):
        super(PanelSource, self).__init__()
        self.params = params
        self.appendable = dict(xs=False, ts=False)
        self.value = None
        self.entities, self.period = None, None
        # self.logger = DataLogHandler()

    def __repr__(self):
        return super(PanelSource, self).__repr__() + str(self.params)

    def __call__(self, entities=None, period: tuple = None):
        return self._run(entities, period)

    def __str__(self):
        return "%s %s" %(self.__class__.__name__, self.params)

    # @DataLogHandler().log_level()
    def _run(self, entities: dict = None, period: tuple = None) -> DataFrame:
        logging.info("RUN " + str(self))
        period_log = (None,None) if period is None else period
        if self.value is None:
            logging.info("RUN INITIAL: [%s] %s - %s", entities, *period_log)
            value = self._wrapped_execute("INITIAL", entities, period)
            self.update(ts=value)
            self.entities, self.period = entities, period
        else:
            appendable_xs, appendable_ts = self.appendable['xs'], self.appendable['ts']
            incremental_period, total_period = self.mismatch_period(period)
            incremental_items, decremental_items, total_items  = self.mismatch_entities(entities)
            
            incremental_period_log, total_period_log = list(map(lambda x: x if x is not None 
                                                else (None,None), (incremental_period, total_period)))
            logging.info("RUN Nth: %s %s - %s", entities, *period_log)
            logging.info("INCREMENTAL Items: %s" %incremental_items)
            logging.info("TOTAL Items: %s" %total_items)
            logging.info("DECREMENTAL Items: %s" %decremental_items)
            logging.info("INCREMENTAL Period: %s - %s", *incremental_period_log)
            logging.info("TOTAL Period: %s - %s", *total_period_log)
            logging.info("APPENDABLE XS:%s TS:%s" %(appendable_xs, appendable_ts))
            
            if appendable_xs and appendable_ts:
                if incremental_items is not None:
                    xs_data = self._wrapped_execute("INCREMENTAL XS1", incremental_items, self.period)
                    self.entities = total_items
                    self.update(xs = xs_data)
                if incremental_period is not None:
                    ts_data = self._wrapped_execute("INCREMENTAL TS1", self.entities, incremental_period)
                    self.period = total_period
                    self.update(ts = ts_data)
            elif appendable_xs and not appendable_ts:
                if period == total_period and incremental_items is not None:
                    xs_data = self._wrapped_execute("INCREMENTAL XS2", incremental_items, self.period)
                    self.entities = total_items
                    self.update(xs = xs_data)
            elif appendable_ts and not appendable_xs:
                if incremental_items is None and decremental_items is None and incremental_period is not None:
                    ts_data = self._wrapped_execute("INCREMENTAL TS2", self.entities, incremental_period)
                    self.period = total_period
                    self.update(ts=ts_data)
            else:
                self.value = self._wrapped_execute("TOTAL", total_items, total_period)
                self.entities, self.period = total_items, total_period

        # TODO: Implement this
        # requested_value = self.subset(entities=entities, period=period)
        requested_value = self.value.copy()
        logging.info("DONE " + str(self))
        return requested_value

    def _wrapped_execute(self, call_type=None, entities=None, period=None) -> DataFrame:
        # with DataLogHandler().log_level()
        period_log = period if period is not None else (None, None)
        logging.info("EXEC %s: [%s] %s - %s" %(call_type, str(entities), *period_log))
        results = self._execute(entities=entities, period=period)
        return results

    @abstractmethod
    def _execute(self, call_type=None, entities=None, period=None) -> DataFrame:
        pass

    # TODO: Convert this method to a Fu
    def mismatch_period(self, period: tuple) -> tuple:
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
                else:
                    incremental = (total[0], self.period[0])
            else: 
                if total[1] > self.period[1]:
                    incremental = (self.period[1], total[1])
                else:  
                    incremental = None

        return incremental, total

    # TODO: Convert this method to a Func
    def mismatch_entities(self, entities: dict) -> tuple:
        if self.entities is None:
            incremental_entities, total_entities = entities, entities
        else:
            initial = [dict(zip(self.entities, x)) for x in itertools.product(*self.entities.values())]
            new = [dict(zip(entities, x)) for x in itertools.product(*entities.values())]
            
            total_entities = dict()
            for level in self.entities.keys():
                s1, s2 = set(self.entities[level]), set(entities[level])
                total_entities[level] = list(s1.union(s2))
            total = [dict(zip(total_entities, x)) for x in itertools.product(*total_entities.values())]
            
            incremental = list(filter(lambda x: x not in initial, total))
            if len(incremental) > 0:
                incremental_entities = dict()
                for level in self.entities.keys():
                    incremental_entities[level] = list(set(map(lambda x: x[level], incremental)))
            else:
                incremental_entities = None
                
            decremental = list(filter(lambda x: x not in new, total))
            if len(decremental) > 0:
                decremental_entities = dict()
            else:
                decremental_entities = None

        return incremental_entities, decremental_entities, total_entities

    def update(self, xs=None, ts=None) -> None:
        if self.value is None:
            if ts is not None:
                self.value = ts
            elif xs is not None:
                self.value = xs
        else:
            if ts is not None:
                self.value = self.value.combine_first(ts)
            if xs is not None:
                self.value = self.value.combine_first(xs)
        
    # TODO: Convert this to a Func
    def subset(self, entities: dict = None, period: tuple = None) -> DataFrame:
        data = None
        if self.value is not None:
            data = self.value.copy()
            if period is not None:
                data = data.truncate(before=period[0], after=period[1])
            if entities is not None:
                # TODO: Create a Func to subset a MultiIndex DataFrame that takes a dict of levels and values
                #   values and returns a subset of the DataFrame
                data = data.subset(entities)
            
        return data

    def reset(self) -> None:
        self.entities, self.period = None, None
        self.value = None

    @staticmethod
    def check_consistency(params: dict = None) -> object:
        pass
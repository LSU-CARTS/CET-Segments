# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 14:26:47 2024

@author: sburt
"""
import numpy as np
import pandas as pd

def cmf_applicator(df, cmf):
    # needs to give a count of all rows where any of the crash attr columns are true (or ==1)
    crash_attrs = cmf.crash_attr

    sev_list = ['100','101','102','103','104']
    if len(crash_attrs)>1:
        filtered_df = df[(df[crash_attrs] == 1).any(axis=1)]
        totals = filtered_df.groupby('CrashSeverityCode').size()
        totals.index = totals.index.astype(str)
        for s in sev_list:
            if s not in totals.index:
                ser = pd.Series({s:0})
                totals = pd.concat([totals,ser])
    else:
        if crash_attrs[0] == 'All':
            filtered_df = df
        else:
            filtered_df = df[(df[crash_attrs] == 1).any(axis=1)]
        totals = filtered_df.groupby('CrashSeverityCode').size()
        totals.index = totals.index.astype(str)
        for s in sev_list:
            if s not in totals.index:
                ser = pd.Series({s: 0})
                totals = pd.concat([totals, ser])

    return totals/len(df.index)

def cmf_adjuster(cmf, severity_percents):
    """
    Gets the final adjusted CMF after accounting for expected percents and applicable severity levels.
    :param cmf: An individual CMF from the cmfs dict.
    :param severity_percents: The severity percents for the applicable highway class/ AADT level combo.
    :return:
    """
    sev_list = ['100', '101', '102', '103', '104']
    # multiply portion(scalar) by percents(series)
    exp_percent = cmf.portion * severity_percents  # get expected percent
    exp_percent.index = sev_list  # set index of expected percent as severity levels
    if cmf.severities[0] != 'All':  # if not all severities are selected, adapt accordingly
        new_sev_list = np.setdiff1d(sev_list, cmf.severities)  # get the severities not selected
        exp_percent.loc[new_sev_list] = [0 for s in new_sev_list]  # zero out severities not selected
    per_veh_effected = sum(exp_percent)
    adj_cmf = ((cmf.cmf - 1) * per_veh_effected) + 1
    return adj_cmf

severity_percents = pd.Series([0.010370,0.00881481, 0.060, 0.3059259, 0.6155556])  # TEMP values for validation

class CMF:
    def __init__(self,cmf,crash_attr,severities,est_cost,cost,srv_life,df):
        self.cmf = cmf
        self.crash_attr = crash_attr
        self.severities = severities
        self.est_cost = est_cost
        self.srv_life = srv_life
        
        percent_dist = cmf_applicator(df,self)
        self.portion = sum(percent_dist)
        self.adj_cmf = cmf_adjuster(self.cmf, severity_percents)
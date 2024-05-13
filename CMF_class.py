# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 14:26:47 2024

@author: sburt
"""
import numpy as np
import pandas as pd
import urllib
import configparser
from cet_funcs import conversion, dummy_wrapper, bca, pv

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
    def __init__(self,cmf,crash_attr,severities,est_cost,srv_life,df):
        translate_dict = {
            "Run off road": "RoadwayDeparture",
            "Fixed object": "ct_G",
            "Rear end": "Mann_Coll_B",
            "Speed Related": "SpeedingRelated",
            "Truck Related": "FMCSAReportableCrash",
            "Wet road": "WET",
            "Nighttime": "DARK",
            "Head on": "Mann_Coll_C",
            "Vehicle/Pedestrian": "Pedestrian",
            "Parking related": "ct_E",
            "Single Vehicle": "SingleVehicle",
            "Vehicle/bicycle": "Bicycle",
            "Angle": "Mann_Coll_D",
            "Multiple vehicle": "MultiVehicle",
            "Left turn": "Left turn",
            "Sideswipe": "Sideswipe",
            "Right turn": "Right turn",
            "Frontal and opposing direction sideswipe": "Mann_Coll_K",
            "Dry weather": "DRY",
            "Day time": "LIGHT",
            "Other": "Mann_Coll_Z",
            "Vehicle/Animal": "ct_N",
            "All": "All"
        }
        self.cmf = cmf
        self.crash_attr = [translate_dict[key] for key in crash_attr]
        self.severities = severities
        self.est_cost = est_cost  # cost for a single service life
        self.srv_life = srv_life
        
        percent_dist = cmf_applicator(df,self)
        self.portion = sum(percent_dist)
        self.adj_cmf = cmf_adjuster(self, severity_percents)
        self.cost = est_cost * full_life/srv_life  # cost for multiple service lives

        self.crf = 1 - self.adj_cmf
        self.crash_reduction = self.crf * exp_crashes
        self.ben_per_year = sum(crash_costs * self.crash_reduction)
        self.total_benefit = -pv(inflation, self.srv_life, self.ben_per_year)
        self.bc_ratio = self.total_benefit/self.est_cost


if __name__ == "__main__":

    doc_string = "069-02_16-18.xlsx"
    df = pd.read_excel(io=doc_string, sheet_name='segment - mod')
    df = conversion(df)
    df = dummy_wrapper(df)

    global config
    config = configparser.ConfigParser()
    config.read('config.ini')
    conn_details = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['CatScan'])
    conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

    cmfs = {
        'cmf1':
        {
            'cmf': 0.825,
            'crash_attr': ['All'],
            'severities': ['All'],
            'est_cost': 60240,
            'srv_life': 5
        },
        'cmf2':
        {
            'cmf':0.887,
            'crash_attr': ['All'],
            'severities': ['All'],
            'est_cost': 66264,
            'srv_life': 5
        },
        'cmf3':
        {
            'cmf': 0.861,
            'crash_attr': ['Wet road'],
            'severities': ['All'],
            'est_cost': 66264,
            'srv_life': 5
        }
    }

    full_life = 20
    inflation = 0.04
    crash_costs = [1710561.00, 489446.00, 173578.00, 58636.00, 24982.00]
    total_crashes = len(df.index)
    crash_years = 1
    exp_crashes = total_crashes * severity_percents / crash_years
    cmf_list = [CMF(*x.values(), df) for x in cmfs.values()]


    [print(y.est_cost) for y in cmf_list]



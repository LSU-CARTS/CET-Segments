# -*- coding: utf-8 -*-
"""
Created on Wed May 22 11:32:40 2024

@author: sburt
"""

import pandas as pd
import numpy as np
import urllib
import configparser
import sqlalchemy
from math import prod
from cet_funcs import conversion, dummy_wrapper, bca, pv

doc_string = "069-02_16-18.xlsx"
df = pd.read_excel(io=doc_string, sheet_name='segment - mod')
df = conversion(df)
df = dummy_wrapper(df)

global config
config = configparser.ConfigParser()
config.read('config.ini')
# server and db info to make the data pull
server = 'SAMLAPTOP'
database = 'CATSCAN'

# connection string
conn_details = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        r"Trusted_Connection=yes"
    )
conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

hwy_class = 'Rural_2-Lane'

def aadt_level(adt, conn_str, conn_str_sam=None):
    """
    Only used when analyzing a segment. Gets the level grouping of AADT: low, med, high.
    :param adt: Traffic measurement of the segment
    :param conn_str: sql connection string
    :param conn_str_sam:
    :return:
    """
    aadt_cutoffs = pd.read_sql("cutoffs", conn_str)


    cutoffs = aadt_cutoffs.loc[aadt_cutoffs.HighwayClass == hwy_class].values[0][1:]
    if adt > cutoffs[1]:
        adt_class = 'high'
    elif adt > cutoffs[0]:
        adt_class = 'med'
    else:
        adt_class = 'low'
    return adt_class

def sev_percents(adt,conn_str):
    level = aadt_level(adt,conn_str)
    level_table = pd.read_sql(level,conn_str)
    percents = level_table[hwy_class][:5]
    return(percents)

print(sev_percents(12500,conn_str))
    


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
from cet_funcs import conversion, dummy_wrapper, bca, pv, aadt_level

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

def sev_percents(adt,conn_str):
    level = aadt_level(adt,conn_str)
    level_table = pd.read_sql(level,conn_str)
    percents = level_table[hwy_class][:5]
    return(percents)

print(sev_percents(12500,conn_str))
    


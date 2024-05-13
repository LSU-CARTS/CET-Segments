# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 15:48:35 2024

@author: sburt
"""

import numpy as np
import pandas as pd

cmf = pd.read_csv('cmfclearinghouse.csv',low_memory=False)

ctype_expansion = cmf['crashType'].str.split(',',expand=True)

Unique_Ctypes = []
for k in ctype_expansion.keys():
    for n in  range(ctype_expansion.shape[0]):
        if ctype_expansion[k][n] not in Unique_Ctypes:
            Unique_Ctypes.append(ctype_expansion[k][n])
            
# ctype_expansion.to_csv('ctypes.csv')

CrashData = pd.read_excel('077-05_17-19.xlsx')

Coll_CDs = [0, 100, 101, 102, 103, 105, 200, 300, 501, 502, 505, 980]
Coll_types = ['NOT A COLLISION BETWEEN TWO MOTOR VEHICLES', 'ANGLE - LEFT OVERTAKE', 'ANGLE - LEFT ACROSS FLOW',
              'ANGLE - RIGHT INTO FLOW', 'ANGLE - LEFT INTO FLOW', 'ANGLE - PERPENDICULAR/OTHER ANGLE',
              ]
Sev_CDs = [100, 101, 102, 103, 104]

y = max(CrashData['CrashDate']).year - min(CrashData['CrashDate']).year + 1

TC = len(CrashData['CrashDate'])

Pvt = pd.pivot_table(CrashData[['MannerCollision','CrashSeverityCode']], index='MannerCollision', columns='CrashSeverityCode',
                     aggfunc=len, fill_value=0, margins=True, dropna=False)

for n in range(len(Coll_CDs)):
    if Coll_CDs[n] not in CrashData['MannerCollisionCode'].unique():
        df1 = Pvt[:n]
        df1.loc[Coll_CDs[n]] = np.zeros(len(Pvt.keys()))
        df2 = Pvt[n:]
        Pvt = pd.concat([df1,df2])
for k in range(len(Sev_CDs)):
    if Sev_CDs[k] not in Pvt.keys():
        Pvt.insert(k, Sev_CDs[k], 0)

Pct = Pvt/TC

CType_Pvt = pd.pivot_table(CrashData[['Crash_Type', 'CrashSeverityCode']], index='Crash_Type', columns='CrashSeverityCode', aggfunc=len, fill_value=0, margins=True, dropna=False)

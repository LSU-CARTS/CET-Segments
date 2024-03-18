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
            
Coll_CDs = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'Z']
Sev_CDs = ['A', 'B', 'C', 'D', 'E']

# ctype_expansion.to_csv('ctypes.csv')

CrashData = pd.read_excel('crash1 I-10 BR Bridge 17-21.xlsx')

y = max(CrashData['CRASH_YEAR']) - min(CrashData['CRASH_YEAR']) + 1

IntCrashData = CrashData[CrashData['INTERSECTION'] == 1]
SegCrashData = CrashData[CrashData['INTERSECTION'] == 0]

TC_Int = len(IntCrashData['INTERSECTION'])
TC_Seg = len(SegCrashData['INTERSECTION'])

Int_pvt = pd.pivot_table(IntCrashData[['MAN_COLL_CD','SEVERITY_CD']], index='MAN_COLL_CD', columns='SEVERITY_CD',
                     aggfunc=len, fill_value=0, margins=True, dropna=False)
Seg_pvt = pd.pivot_table(SegCrashData[['MAN_COLL_CD','SEVERITY_CD']], index='MAN_COLL_CD', columns='SEVERITY_CD',
                     aggfunc=len, fill_value=0, margins=True, dropna=False)


for n in range(len(Coll_CDs)):
    if Coll_CDs[n] not in IntCrashData['MAN_COLL_CD'].unique():
        df1 = Int_pvt[:n]
        df1.loc[Coll_CDs[n]] = np.zeros(len(Int_pvt.keys()))
        df2 = Int_pvt[n:]
        Int_pvt = pd.concat([df1,df2])
for k in range(len(Sev_CDs)):
    if Sev_CDs[k] not in Int_pvt.keys():
        Int_pvt.insert(k, Sev_CDs[k], 0)

Int_pct = Int_pvt/TC_Int
Seg_pct = Seg_pvt/TC_Seg


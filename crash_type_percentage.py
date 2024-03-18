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

CrashData = pd.read_excel('CET - CSect 069-02 LM 5.89 - 8.902.xlsm', 'Crashes (COPY TO)')

y = max(CrashData['CRASH YEAR']) - min(CrashData['CRASH YEAR']) + 1

IntCrashData = CrashData[CrashData['INTERSECTION'] == 1]
SegCrashData = CrashData[CrashData['INTERSECTION'] == 0]

TC_Int = len(IntCrashData['INTERSECTION'])
TC_Seg = len(SegCrashData['INTERSECTION'])

Int_pvt = pd.pivot_table(IntCrashData[['MAN COLL CD','SEVERITY CD']], index='MAN COLL CD', columns='SEVERITY CD',
                     aggfunc=len, fill_value=0, margins=True)
Seg_pvt = pd.pivot_table(SegCrashData[['MAN COLL CD','SEVERITY CD']], index='MAN COLL CD', columns='SEVERITY CD',
                     aggfunc=len, fill_value=0, margins=True)

Int_pct = Int_pvt/TC_Int
Seg_pct = Seg_pvt/TC_Seg

print(Int_pct.to_string())
print(Seg_pct.to_string())
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 10:08:29 2024

@author: sburt
"""

import numpy as np

#Placeholder values for testing
cmf = 0.825
ExistingCrashes = [0.09, 0.07, 0.54, 2.75, 5.54]
CrashCosts = [1710561.00, 489446.00, 173578.00, 58636.00, 24982.00]
y = 20
Inflation = 0.04
EstimatedCost = 240960


CrashReduction = ExistingCrashes - np.multiply(cmf,ExistingCrashes)
BenefitsPerYear = np.dot(CrashReduction,CrashCosts)

# CostsPerYear = np.dot(ExistingCrashes,CrashCosts)

def pv(r: float, n: int, pmt):
    # takes in rate (r), number of periods (n), and payment size (pmt)
    # future value (f) is not needed and therefore will always be 0
    # 'when' (or: when payments are made, beginning or end of period (w)) is not needed, will always be 0
    f = 0
    w = 0
    present_v = ((r + 1) ** (-n)*(-f * r - pmt * ((r + 1)**n - 1) * (r*w + 1)))/r
    return present_v

SL_Benefit = -pv(Inflation, y, BenefitsPerYear)
BenCost = SL_Benefit/EstimatedCost

print(SL_Benefit)
print(BenCost)
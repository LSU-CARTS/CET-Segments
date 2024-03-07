import numpy as np
import pandas as pd

cmf = pd.read_csv('cmfclearinghouse.csv',low_memory=False)

ctype_expansion = cmf['crashType'].str.split(',',expand=True)

print(ctype_expansion)

# ctype_expansion.to_csv('ctypes.csv')


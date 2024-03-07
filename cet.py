import pandas as pd
import numpy as np
import math

df = pd.read_excel(io='SiegenExample.xlsx',sheet_name='Sheet1')
years = 5

print(df.columns)

# print(df.CrashSeverity.value_counts())
# print(df.MannerCollision.value_counts())

# trying to get a matrix counting crash severities and manners of collision

# crosstab
columns = ['MannerCollision','RoadwayDeparture','Pedestrian','Bicycle']

cross = pd.crosstab(columns=[df[col] for col in columns],index=[df.CrashSeverity, df.IsIntersection],
                    colnames=columns,rownames=['CrashSeverity', 'IsIntersection'])

# get rate of crashes for each cross category per year.
# Just divide the whole table by number of years in the data.
cross_per_year = cross/years
# print(cross_per_year.to_string())
cross_per_year.to_excel('cross_per_year.xlsx')

# Table Totals from cross_per_year figures by IsIntersection
cross_py_reset_index = cross_per_year.reset_index()
cross_per_year_seg_total = cross_py_reset_index[cross_py_reset_index['IsIntersection']=='NO'].sum()[2:].sum()
cross_per_year_int_total = cross_py_reset_index[cross_py_reset_index['IsIntersection']=='YES'].sum()[2:].sum()

# Cross Percentages
# Gets the totals from the per_years rates and multiplies them by the number of years to get a total figure.
# this becomes the denominator of a fraction with the numerator still being the count figures from the first crosstable
cross_percent_seg = cross[cross.index.get_level_values('IsIntersection')=='NO']/(years * cross_per_year_seg_total)
cross_percent_int = cross[cross.index.get_level_values('IsIntersection')=='NO']/(years * cross_per_year_int_total)

cross_percent_seg.to_excel('segment_percent.xlsx')
cross_percent_int.to_excel('intersection_percent.xlsx')
import pandas as pd

mann_coll_list = ['000', '-1', '100', '101', '102', '103', '104', '105', '200', '201', '202', '300', '400', '401',
                  '402', '500', '501', '502', '503', '504', '505', '980', '999']

doc_string = "C:/Users/malle72/projects/CAT Scan/077-05_17-19.xlsx"
df = pd.read_excel(doc_string)

pvt = pd.pivot_table(df[['MannerCollisionCode', 'CrashSeverityCode']],
                     index='MannerCollisionCode',
                     columns='CrashSeverityCode',
                     aggfunc=len,
                     fill_value=0,
                     margins=True)
# values=['CrashPK'], , margins=True
pvt.index = pvt.index.astype(str)

missing_mann_coll = [m for m in mann_coll_list if m not in pvt.index]

for item in missing_mann_coll:
    pvt.loc[item] = 0

pvt = pvt.sort_index()

print(pvt)

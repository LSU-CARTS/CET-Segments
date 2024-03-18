import pandas as pd


doc_string = "C:/Users/malle72/projects/CAT Scan/077-05_17-19.xlsx"
df = pd.read_excel(doc_string)

pvt = pd.pivot_table(df[['MannerCollisionCode','CrashSeverityCode']],
                     index='MannerCollisionCode',
                     columns='CrashSeverityCode',
                     aggfunc=len,
                     fill_value=0,
                     margins=True)
# values=['CrashPK'], , margins=True
print(pvt)

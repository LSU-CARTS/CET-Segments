import pandas as pd


doc_string = "C:/Users/malle72/projects/CAT Scan/077-05_17-19.xlsx"
df = pd.read_excel(doc_string)

pvt = pd.pivot_table(df, index=['MannerCollisionCode'],columns=['CrashSeverityCode']
                     , aggfunc="size", fill_value=0)

print(pvt)
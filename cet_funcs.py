import pandas as pd
import numpy as np
import urllib
import configparser
from math import prod

def dummies(row,vals,final_col_names=None):
    """
    To be used in a .apply() method on a pandas Series. Creates dummy variables.
    :param row: Series: Column from dataframe for which you want to make dummy variables
    :param vals: List: items from col for which you want to make dummy vars. Can also be a list of lists to group multiple values.
    :param final_col_names: List: column names for final output. Optional.
    :return: dataframe or collection of series objects
    Output needs to be converted to dataframe then merged to the origin dataframe.
    """
    # TODO Improve this. It's very bulky and is inefficient since it throws dataframes and series around
    width=len(vals)

    # address multiple groupings
    col_names = []
    for i in vals:
        if type(i) is list:
            i = "".join(x for x in i)
        col_names.append(i)

    # create empty dataframe and pass column names appropriately
    if final_col_names:
        # if vals and final_col_names aren't the same length, throw error.
        if len(vals) != len(final_col_names):
            ValueError("Grouped values and column names passed must be the same length.")
        df_dummy = pd.DataFrame(columns=final_col_names)
    else:
        df_dummy = pd.DataFrame(columns=col_names)

    dummy_row = [0 for y in range(width)]

    # get index of matched row value in vals list (this index will match up with the column that should equal 1
    for i, sublist in enumerate(vals):
        if row in sublist:
            dummy_row[i] = 1

    df_dummy.loc[len(df_dummy)] = dummy_row

    return df_dummy

def conversion(df):
    # dict to assign old codes to new codes
    # need string versions of each item in lists to account for variable datatypes in input
    df['MannerCollisionCode'] = df['MannerCollisionCode'].astype(str)
    man_coll_dict = {'A': ['000', '0'], 'B': ['300'], 'C': ['200'], 'D': ['105'],
                     'E': ['100', '503'], 'F': ['101'],
                     'G': ['102'], 'H': ['103'], 'I': ['501', '202'], 'J': ['505'], 'K': ['502'],
                     'Z': ['980', '999', '-1', '104', '201', '400', '401', '402', '500', '504']}

    # applying the above defined dictionary to the new collision codes to create a column of the old collision codes
    df['OldMannerCollisionCode'] = df['MannerCollisionCode'].apply(
        lambda x: [a for a, b in man_coll_dict.items() if x in b][0])

    return df

def manner_severity_percents(df):
    """
    DEPRECIATED; Not needed since percents are dynamically calculated cmf_applicator and cmf_adjuster.
    Creates pivot table that counts intersection between severity and crash manner values.
    Adds total row and column.
    Ensures that all valid values of crash manner and severity are present in the index and columns.
    :param df: crash data
    :return: pivot table with percentage of crashes represented by each manner/severity pair.
    """
    # mann_coll_list = ['000', '-1', '100', '101', '102', '103', '104', '105', '200', '201', '202', '300', '400', '401',
    #                   '402', '500', '501', '502', '503', '504', '505', '980', '999']

    inj_severity_list = ['-1','100','101','102','103','104','999']
    mann_coll_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'Z']

    pvt = pd.pivot_table(df[['OldMannerCollisionCode', 'SeverityCode']],
                         index='OldMannerCollisionCode',
                         columns='SeverityCode',
                         aggfunc=lambda x: len(x) / len(df),  # custom aggfunc to get % of total each pair represents
                         fill_value=0,
                         margins=True)

    # convert index and columns to strings
    pvt.index = pvt.index.astype(str)
    pvt.columns = pvt.columns.astype(str)

    # check if all values in master lists are in the index/columns
    missing_mann_coll = [m for m in mann_coll_list if m not in pvt.index]
    missing_inj = [i for i in inj_severity_list if i not in pvt.columns]

    # add rows or columns of zeros for values that were missing
    for item in missing_mann_coll:
        pvt.loc[item] = 0

    for col in missing_inj:
        pvt[col] = 0

    # ensure the totals row/column are at the outside
    idx = pvt.index.tolist()
    idx.remove('All')
    pvt.reindex(idx.append('All'))

    pvt = pvt.sort_index(axis=1)

    return round(pvt, 4)

def filler(df):
    """
    For any crash types or manners of collision that may not have occurred in the data, ensure there is a binary column
    for it.
    :param df: crash data
    :return: dataframe with any columns left out by dummy function.
    """
    # All Crash Types
    ct_list = ['ct_A','ct_B','ct_C','ct_D','ct_E','ct_F','ct_G','ct_H','ct_J','ct_K','ct_M','ct_N','ct_P','ct_Q','ct_R','ct_S','ct_T','ct_X']
    # All Manners of Collision
    mann_coll_list = ['Mann_Coll_A', 'Mann_Coll_B', 'Mann_Coll_C', 'Mann_Coll_D', 'Mann_Coll_E', 'Mann_Coll_F', 'Mann_Coll_G', 'Mann_Coll_H', 'Mann_Coll_I', 'Mann_Coll_J', 'Mann_Coll_K', 'Mann_Coll_Z']
    for i in ct_list:
        if i not in df.columns:
            df[i] = 0
    for j in mann_coll_list:
        if j not in df.columns:
            df[j] = 0

    return df


def dummy_wrapper(df):
    """
    Wrapper function that gets the dummy variables for the Road Surface, Lighting, and Manner of Collision.
    :param df: crash data
    :return: dataframe with dummy columns appended
    """
    # get dummy vars for Road Surface
    surf_dummy_vals = [['0','000','980'],['106','107','100','104']]  # grouping of surface condition values
    surf_dummy_cols = ['DRY','WET']  # surf condition group titles. Dummy column names
    # Create dummies
    surf_dummies = df['SurfaceConditionCode'].apply(dummies, args=[surf_dummy_vals,surf_dummy_cols])
    surf_result = pd.concat(surf_dummies.tolist(), ignore_index=True)  # manipulate dummy output into correct format

    # get dummy vars for Light Condition (same process as Road Surface)
    light_dummy_vals = [['100','200'],['301','302','300']]
    light_dummy_cols = ['LIGHT','DARK']
    light_dummies = df['LightingCode'].apply(dummies, args=[light_dummy_vals,light_dummy_cols])
    light_result = pd.concat(light_dummies.tolist(), ignore_index=True)

    # custom dummies for Manner Collision
    mann_coll_vals = [['E','F','G'], ['H','I'], ['J','K']]
    mann_coll_cols = ['Left turn', 'Right turn', 'Sideswipe']
    mann_coll_dummies = df['OldMannerCollisionCode'].apply(dummies, args=[mann_coll_vals,mann_coll_cols])
    mann_coll_result = pd.concat(mann_coll_dummies.tolist(), ignore_index=True)

    # get dummies for Manner Collision
    df = pd.get_dummies(df,columns=['OldMannerCollisionCode'], prefix='Mann_Coll')  # Can just use regular pandas dummy func for this

    df = pd.get_dummies(df, columns=['CrashType'], prefix='ct')

    df_out = pd.concat(objs=[df,surf_result,light_result,mann_coll_result],axis=1)  # combine all dummy fields + df
    df_out = filler(df_out)  # get zero-value dummy columns for values that don't appear in mann coll or crash type

    return df_out


def crash_attr_translate(cmf: dict):
    # needs to be able to dynamically build filtering criteria
    # needs edit and return each entire cmf dictionary entry
    translate_dict ={
        "Run off road": "RoadwayDeparture",
        "Fixed object": "ct_G",
        "Rear end": "Mann_Coll_B",
        "Speed Related": "SpeedingRelated",
        "Truck Related": "FMCSAReportableCrash",
        "Wet road": "WET",
        "Nighttime": "DARK",
        "Head on": "Mann_Coll_C",
        "Vehicle/Pedestrian": "Pedestrian",
        "Parking related": "ct_E",
        "Single Vehicle": "SingleVehicle",
        "Vehicle/bicycle": "Bicycle",
        "Angle": "Mann_Coll_D",
        "Multiple vehicle": "MultiVehicle",
        "Left turn": "Left turn",
        "Sideswipe": "Sideswipe",
        "Right turn": "Right turn",
        "Frontal and opposing direction sideswipe": "Mann_Coll_K",
        "Dry weather": "DRY",
        "Day time": "LIGHT",
        "Other": "Mann_Coll_Z",
        "Vehicle/Animal": "ct_N",
        "All": "All"
    }

    converted_cols = [translate_dict[key] for key in cmf['crash_attr']]
    cmf['crash_attr'] = converted_cols  # this gets applied in place; no need to return anything
    # return cmf


def aadt_level(adt, hwy_class, conn_str):
    """
    Only used when analyzing a segment. Gets the level grouping of AADT: low, med, high.
    :param adt: Traffic measurement of the segment
    :param conn_str: sql connection string
    :return:
    """
    aadt_cutoffs = pd.read_sql("cutoffs", conn_str)

    cutoffs = aadt_cutoffs.loc[aadt_cutoffs.HighwayClass == hwy_class].values[0][1:]
    if adt > cutoffs[1]:
        adt_class = 'high'
    elif adt > cutoffs[0]:
        adt_class = 'med'
    else:
        adt_class = 'low'
    return adt_class


def get_state_percents(adt_class, hwy_class, conn_str):
    """
    Will need to be different for intersection CET
    :param adt_level: low, med, high
    :param conn_str:
    :param conn_str_sam:
    :return:
    """
    state_percents = pd.read_sql(adt_class, conn_str)

    # select rows relevant to severity
    severity_state_percents = state_percents.iloc[0:5]
    # select rows relevant to crash types
    crash_state_percents = state_percents.iloc[5:24]
    # select rows relevant to manners of collision
    manner_state_percent = state_percents.iloc[24:36]
    # select rows relevant to other crash factors
    other_state_percents = state_percents.iloc[44:]
    return severity_state_percents[hwy_class]


def cmf_applicator(df, cmf: dict):
    # needs to give a count of all rows where any of the crash attr columns are true (or ==1)
    crash_attrs = cmf['crash_attr']

    sev_list = ['100','101','102','103','104']
    if len(crash_attrs)>1:
        filtered_df = df[(df[crash_attrs] == 1).any(axis=1)]
        totals = filtered_df.groupby('SeverityCode').size()
        totals.index = totals.index.astype(str)
        for s in sev_list:
            if s not in totals.index:
                ser = pd.Series({s:0})
                totals = pd.concat([totals,ser])
    else:
        if crash_attrs[0] == 'All':
            filtered_df = df
        else:
            filtered_df = df[(df[crash_attrs] == 1).any(axis=1)]
        totals = filtered_df.groupby('SeverityCode').size()
        totals.index = totals.index.astype(str)
        for s in sev_list:
            if s not in totals.index:
                ser = pd.Series({s: 0})
                totals = pd.concat([totals, ser])

    return totals/len(df.index)


def cmf_adjuster(cmf:dict, severity_percents):
    """
    Gets the final adjusted CMF after accounting for expected percents and applicable severity levels.
    :param cmf: An individual CMF from the cmfs dict.
    :param severity_percents: The severity percents for the applicable highway class/ AADT level combo.
    :return:
    """
    sev_list = ['100', '101', '102', '103', '104']
    # multiply portion(scalar) by percents(series)
    exp_percent = cmf['portion'] * severity_percents  # get expected percent
    exp_percent.index = sev_list  # set index of expected percent as severity levels
    if cmf['severities'][0] != 'All':  # if not all severities are selected, adapt accordingly
        new_sev_list = np.setdiff1d(sev_list, cmf['severities'])  # get the severities not selected
        exp_percent.loc[new_sev_list] = [0 for s in new_sev_list]  # zero out severities not selected
    per_veh_effected = sum(exp_percent)
    adj_cmf = ((cmf['cmf'] - 1) * per_veh_effected) + 1
    return adj_cmf


def pv(r: float, n: int, pmt):
    """
    Takes in rate (r), number of periods (n), and payment size (pmt)
    Future value (f) is not needed and therefore will always be 0
    'when' (or: when payments are made, beginning or end of period (w)) is not needed, will always be 0
    :param r: rate. In this project, always inflation.
    :param n: number of periods. In this project, service life.
    :param pmt: Payment size. In this project, monetary benefit of countermeasure.
    :return:
    """
    f = 0.0
    w = 0.0
    present_v = ((r + 1) ** (-n)*(-f * r - pmt * ((r + 1)**n - 1) * (r*w + 1)))/r
    return -present_v

def bca(final_cmf, crashes_per_yr, cm_cost, srv_life, inflation):
    crash_costs = [1710561.00, 489446.00, 173578.00, 58636.00, 24982.00]  # TEMP Values
    crf = 1 - final_cmf

    crash_reduction = crf * crashes_per_yr
    benefits_per_yr = sum(crash_costs * crash_reduction)
    total_benefit = pv(inflation, srv_life, benefits_per_yr)
    bc_ratio = total_benefit/cm_cost

    return benefits_per_yr, total_benefit, bc_ratio

if __name__ == "__main__":

    # This section for testing only. Not for production use.
    print("=====Running Test=====")

    doc_string = "069-02_16-18.xlsx"
    df = pd.read_excel(io=doc_string,sheet_name='segment - mod')

    global config
    config = configparser.ConfigParser()
    config.read('config.ini')
    server = 'SAMLAPTOP'
    database = 'CATSCAN'
    conn_details = (
            r"Driver={ODBC Driver 17 for SQL Server};"
            f"Server={server};"
            f"Database={database};"
            r"Trusted_Connection=yes"
        )
    conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

    # ==============Sam's conn details here=================

    # ==============INPUTS==================================
    hwy_class = 'Rural_2-Lane'
    cmfs = {
        'cmf1':
        {
            'cmf': 0.825,
            'crash_attr': ['All'],
            'severities': ['All'],
            'est_cost': 60240,
            'cost': 240960,
            'srv_life': 5
        },
        'cmf2':
        {
            'cmf':0.887,
            'crash_attr': ['All'],
            'severities': ['All'],
            'est_cost': 66264,
            'cost': 265056,
            'srv_life': 5
        },
        'cmf3':
        {
            'cmf': 0.861,
            'crash_attr': ['Wet road'],
            'severities': ['All'],
            'est_cost': 66264,
            'cost': 265056,
            'srv_life': 5
        }
    }

    crash_years = 3
    srv_life = 20
    cm_cost = sum([cmfs[cmf]['cost'] for cmf in cmfs])

    adt = 12500
    adt_class = aadt_level(adt,conn_str)

    # severity_percents = get_state_percents(adt_class,hwy_class,conn_str)
    severity_percents = pd.Series([0.0103703703703704,0.00814814814814815, 0.060, 0.305925925925926, 0.615555556], dtype=pd.Float64Dtype())  # TEMP values for validation
    # ===================END INPUTS=====================================

    df = conversion(df)
    df = dummy_wrapper(df)
    total_crashes = len(df.index)
    exp_crashes = total_crashes*severity_percents/crash_years  # vector of expected crashes per severity level

    for cmf in cmfs: crash_attr_translate(cmfs[cmf])
    percent_dist = [cmf_applicator(df, cmfs[x]) for x in cmfs]  # applicable crashes/total crashes per severity level

    for t,cmf in zip(percent_dist,cmfs):  # need to add item to each CMF dict for sum of each item in percent_dist
        cmfs[cmf]['portion'] = sum(t)
        cmfs[cmf]['adj_cmf'] = cmf_adjuster(cmfs[cmf], severity_percents)  # after this each cmf has a list of expected percents per severity
        # these need to be filtered by applicable severity levels for that CMF and then summed.
    print(cmfs)
    
    for cmf in cmfs.keys():
        benefits_per_yr, total_benefit, bc_ratio = bca(cmfs[cmf]['adj_cmf'], exp_crashes, cmfs[cmf]['est_cost'], cmfs[cmf]['srv_life'], inflation=0.04)
        print(f"=========={cmf}==========")
        print(f"\nBenefits per Year: \n{benefits_per_yr}")
        print(f"\nTotal Expected Benefit: \n{total_benefit}")
        print(f"\nEstimated Cost of Countermeasure: \n{cmfs[cmf]['est_cost']}")
        print(f"\nBenefit/Cost Ratio: \n{bc_ratio}")
    
    combined_cmf = prod([cmfs[cmf]['adj_cmf'] for cmf in cmfs])
    print("\nCombined CMF: ", combined_cmf)

    benefits_per_yr, total_benefit, bc_ratio = bca(combined_cmf, exp_crashes, cm_cost, srv_life)
    print(f"Total Crashes: {total_crashes}")
    print(f"Expected Crashes: \n{exp_crashes}")
    print(f"\nBenefits per Year: \n{benefits_per_yr}")
    print(f"\nTotal Expected Benefit: \n{total_benefit}")
    print(f"\nExpected Cost of Countermeasure: \n{cm_cost}")
    print(f"\nBenefit/Cost Ratio: \n{bc_ratio}")

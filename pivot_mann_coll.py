import pandas as pd

def dummies(row,vals,final_col_names=None):
    """
    To be used in a .apply() method on a pandas Series. Creates dummy variables.
    :param row: Series: Column from dataframe for which you want to make dummy variables
    :param vals: List: items from col for which you want to make dummy vars. Can also be a list of lists to group multiple values.
    :param final_col_names: List: column names for final output. Optional.
    :return: dataframe or collection of series objects
    Output needs to be converted to dataframe then merged to the origin dataframe.
    """

    width=len(vals)

    # address multiple groupings
    col_names = []
    for i in vals:
        if type(i) == list:
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
    man_coll_dict = {'A': ['000', '0'], 'B': ['300', '400', '401', '402'], 'C': ['200', '201', '202'], 'D': ['105'],
                     'E': ['100'], 'F': ['101', '500'],
                     'G': ['102'], 'H': ['103', '104'], 'I': ['501'], 'J': ['505'], 'K': ['502', '503', '504'],
                     'Z': ['980', '999', '-1']}

    # applying the above defined dictionary to the new collision codes to create a column of the old collision codes
    df['OldMannerCollisionCode'] = df['MannerCollisionCode'].apply(
        lambda x: [a for a, b in man_coll_dict.items() if x in b][0])

    return df

def manner_severity_percents(df):
    """
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

    pvt = pd.pivot_table(df[['OldMannerCollisionCode', 'CrashSeverityCode']],
                         index='OldMannerCollisionCode',
                         columns='CrashSeverityCode',
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
    surf_dummy_vals = [['DRY','OTHER'],['WET','ICE/FROST','SLUSH']]  # grouping of surface condition values
    surf_dummy_cols = ['DRY','WET']  # surf condition group titles. Dummy column names
    # Create dummies
    surf_dummies = df['RoadwaySurfaceCondition'].apply(dummies, args=[surf_dummy_vals,surf_dummy_cols])
    surf_result = pd.concat(surf_dummies.tolist(), ignore_index=True)  # manipulate dummy output into correct format

    # get dummy vars for Light Condition (same process as Road Surface)
    light_dummy_vals = [['DAYLIGHT','DAWN/DUSK'],['DARK - STREET LIGHTS AT INTERSECTION ONLY','DARK - NOT LIGHTED','DARK - CONTINUOUS STREET LIGHTS']]
    light_dummy_cols = ['LIGHT','DARK']
    light_dummies = df['LightCondition'].apply(dummies, args=[light_dummy_vals,light_dummy_cols])
    light_result = pd.concat(light_dummies.tolist(), ignore_index=True)

    # custom dummies for Manner Collision
    mann_coll_vals = [['E','F','G'], ['H','I'], ['J','K']]
    mann_coll_cols = ['Left turn', 'Right turn', 'Sideswipe']
    mann_coll_dummies = df['OldMannerCollisionCode'].apply(dummies, args=[mann_coll_vals,mann_coll_cols])
    mann_coll_result = pd.concat(mann_coll_dummies.tolist(), ignore_index=True)

    # get dummies for Manner Collision
    df = pd.get_dummies(df,columns=['OldMannerCollisionCode'], prefix='Mann_Coll')  # Can just use regular pandas dummy func for this

    df = pd.get_dummies(df, columns=['Crash_Type'], prefix='ct')

    # dummy_result = pd.merge(surf_result, light_result, left_index=True, right_index=True)  # combine dummy dfs
    # df_out = pd.merge(df, dummy_result, left_index=True, right_index=True)  # add dummy df to output df
    df_out = pd.concat(objs=[df,surf_result,light_result,mann_coll_result],axis=1)  # combine all dummy fields + df
    df_out = filler(df_out)  # get zero-value dummy columns for values that don't appear in mann coll or crash type

    return df_out


# TODO Function to build criteria based on the cmf selection and applicable crash types
def crash_attr_translate(cmf: dict):
    # needs to be able to dynamically build filtering criteria
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
        "Vehicle/Animal": "ct_N"
    }

    converted_cols = [translate_dict[key] for key in cmf['crash_attr']]

    return converted_cols

def cmf_applicator():
    pass

if __name__ == "__main__":
    doc_string = "077-05_17-19.xlsx"
    df = pd.read_excel(doc_string)

    cmfs = [
        {
            'cmf1': 0.9,
            'crash_attr': ['Head on','Wet road', 'Nighttime']
        },
        {
            'cmf2':0.8,
            'crash_attr': ['Angle', 'Day time']
        }
    ]

    # df = conversion(df)
    # df = dummy_wrapper(df)
    translated_crash_attrs = [crash_attr_translate(cmf) for cmf in cmfs]

    print(translated_crash_attrs)

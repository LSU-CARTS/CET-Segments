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

    # get dummies for Manner Collision
    df = pd.get_dummies(df,columns=['OldMannerCollisionCode'], prefix='Mann_Coll')  # Can just use regular pandas dummy func for this

    dummy_result = pd.merge(surf_result, light_result, left_index=True, right_index=True)  # combine dummy dfs
    df_out = pd.merge(df, dummy_result, left_index=True, right_index=True)  # add dummy df to output df

    return df_out

# TODO Need function to handle percentages for Crash Types

# TODO Need function to handle final table


if __name__ == "__main__":
    doc_string = "077-05_17-19.xlsx"
    df = pd.read_excel(doc_string)
    df = conversion(df)
    df = dummy_wrapper(df)
    print(df.columns)

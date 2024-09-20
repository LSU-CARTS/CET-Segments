
import numpy as np
import pandas as pd
import urllib
import configparser
from cet_funcs import conversion, dummy_wrapper, bca, pv, aadt_level, get_state_percents

def cmf_applicator(df, cmf):
    # needs to give a count of all rows where any of the crash attr columns are true (or ==1)
    crash_attrs = cmf.crash_attr

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

def cmf_adjuster(cmf, severity_percents):
    """
    Gets the final adjusted CMF after accounting for expected percents and applicable severity levels.
    :param cmf: An individual CMF from the cmfs dict.
    :param severity_percents: The severity percents for the applicable highway class/ AADT level combo.
    :return:
    """
    sev_list = ['100', '101', '102', '103', '104']
    # multiply portion(scalar) by percents(series)
    exp_percent = cmf.portion * severity_percents  # get expected percent
    exp_percent.index = sev_list  # set index of expected percent as severity levels
    if cmf.severities[0] != 'All':  # if not all severities are selected, adapt accordingly
        new_sev_list = np.setdiff1d(sev_list, cmf.severities)  # get the severities not selected
        exp_percent.loc[new_sev_list] = [0 for s in new_sev_list]  # zero out severities not selected
    per_veh_effected = sum(exp_percent)
    adj_cmf = ((cmf.cmf - 1) * per_veh_effected) + 1
    return adj_cmf

class CMF:
    def __init__(self,id,id2,desc,cmf,crash_attr,severities,est_cost,srv_life,full_life, exp_crashes, severity_percents, crash_costs, inflation,df):
        """

        :param id: countermeasure ID
        :param cmf: crash modification factor
        :param desc: name/description of the countermeasure
        :param crash_attr: attributes of crashes to which this cm is applicable
        :param severities: crash severity levels to which this cm is applicable
        :param est_cost: estimated cost of this cm
        :param srv_life: estimated service life of this cm
        :param full_life: how long we would like this cm to be viable. (usually a multiple of the service life)
        :param exp_crashes: expected crashes on the road being analyzed
        :param severity_percents: state background percents for each severity level for current hwy class
        :param crash_costs: dollar value of economic cost per severity level
        :param inflation: inflation figure for present value calculation
        :param df: crash data
        """
        translate_dict = {
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
        # Basic attributes
        self.id = id
        self.id2 = id2
        self.cmf = cmf
        self.desc = desc
        self.crash_attr = [translate_dict[key] for key in crash_attr]
        self.severities = severities
        self.est_cost = est_cost  # cost for a single service life
        self.srv_life = srv_life
        self.full_life = full_life
        # Calculated attributes
        percent_dist = cmf_applicator(df,self)
        self.portion = sum(percent_dist)
        self.adj_cmf = cmf_adjuster(self, severity_percents)
        self.cost = est_cost * full_life/srv_life  # cost for multiple service lives
        # BCA attributes
        self.crf = 1 - self.adj_cmf
        self.crash_reduction = self.crf * exp_crashes
        self.ben_per_year = round(sum(crash_costs * self.crash_reduction), 2)
        self.total_benefit = round(-pv(inflation, self.srv_life, self.ben_per_year), 2)
        self.bc_ratio = round(self.total_benefit/self.est_cost, 3)


if __name__ == "__main__":

    doc_string = "069-02_16-18.xlsx"
    df = pd.read_excel(io=doc_string, sheet_name='segment - mod')
    df = conversion(df)
    df = dummy_wrapper(df)
    sam_bool = False

    global config
    config = configparser.ConfigParser()
    config.read('config.ini')
    # server and db info to make the data pull
    conn_details = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['CatScan'])
    conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'
    if sam_bool:
        server = 'SAMLAPTOP'
        database = 'CATSCAN'

        # connection string
        conn_details = (
                r"Driver={ODBC Driver 17 for SQL Server};"
                f"Server={server};"
                f"Database={database};"
                r"Trusted_Connection=yes"
            )
        conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

    cmfs = {
        '4736':
            {
                'cmf': 0.825,
                'desc': 'description of CMF1',
                'crash_attr': ['All'],
                'severities': ['All'],
                'est_cost': 60240,
                'srv_life': 5
            },
        '8101':
            {
                'cmf': 0.887,
                'desc': 'description of CMF2',
                'crash_attr': ['All'],
                'severities': ['All'],
                'est_cost': 66264,
                'srv_life': 5
            },
        '8137':
            {
                'cmf': 0.861,
                'desc': 'description of CMF3. Wet roads',
                'crash_attr': ['Wet road'],
                'severities': ['All'],
                'est_cost': 66264,
                'srv_life': 5
            }
    }

    full_life_set = 20
    inflation = 0.04
    crash_costs = [1710561.00, 489446.00, 173578.00, 58636.00, 24982.00]
    total_crashes = len(df.index)
    crash_years = 3
    seg_len = 3.012
    exp_crash_mi_yr = 2.986707023876080
    severity_percents = pd.Series([0.01037037037,0.008148148, 0.060, 0.3059259259, 0.615555556])  # TEMP values for validation
    exp_crashes_set = exp_crash_mi_yr * seg_len * severity_percents
    hwy_class = 'Rural_2-Lane'
    adt = 12500
    srv_life = 20

    adt_class = aadt_level(adt, hwy_class, conn_str)

    severity_percents = get_state_percents(adt_class,hwy_class,conn_str)
    
    # exp_crashes_set = total_crashes / crash_years * severity_percents
    ref_metrics = [full_life_set, exp_crashes_set, severity_percents, crash_costs, inflation]

    cmf_list = [CMF(x, *y.values(), *ref_metrics, df) for x,y in zip(cmfs.keys(),cmfs.values())]

    cmf_dict = {c.id: {'Description':c.desc,
                       'Est Cost':c.est_cost,
                       'Srv Life': c.srv_life,
                       'Benefits/Yr':c.ben_per_year,
                       'Benefit/Cost Ratio':c.bc_ratio,
                       'Expected Service Life Benefits':c.total_benefit} for c in cmf_list}
    # print(cmf_dict)
    print(exp_crash_mi_yr * seg_len)
    print(exp_crashes_set)
    out_df = pd.DataFrame.from_dict(cmf_dict,orient='index')
    print(out_df.to_string())
    # [print(y.id) for y in cmf_list]

    combined_cmf = 1
    cm_cost = 0
    for c in cmf_list:
        combined_cmf *= c.adj_cmf
        cm_cost += c.cost
    print('Combined CMF: '+str(combined_cmf))
    
    benefits_per_yr, total_benefit, bc_ratio = bca(combined_cmf, exp_crashes_set, cm_cost, srv_life, inflation=0.04)
    print(f"Total Crashes: {total_crashes}")
    print(f"Expected Crashes: \n{exp_crashes_set}")
    print(f"\nBenefits per Year: \n{benefits_per_yr}")
    print(f"\nTotal Expected Benefit: \n{total_benefit}")
    print(f"\nExpected Cost of Countermeasure: \n{cm_cost}")
    print(f"\nBenefit/Cost Ratio: \n{bc_ratio}")


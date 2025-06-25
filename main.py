import sys
import os
from collections import Counter
import pandas as pd
import pika
import json
import urllib
import uuid
import configparser
from math import prod
import traceback
from CMF_class import CMF
from cet_funcs import conversion, dummy_wrapper, get_state_percents, pv, aadt_level

def publish_event(channel, method, body,exchange):
    channel.basic_publish(exchange=exchange, routing_key='' ,body=body)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def publish_statusevent(channel,body,exchange):
    channel.basic_publish(exchange=exchange, routing_key='' ,body=body)

# def log_func(data, clientID, projectID, error_stack, error):
#     """
#     Logging function to handle errors anywhere in the CAT Scan function (cat_int)
#     :param data: the json data object received from Crash Query Tool
#     :param clientID: client ID from CQT
#     :param projectID: project ID from CQT
#     :param error_stack: full error stack for whatever error occurred
#     :param error: one line error statement
#     :return: posts details of the error to IntCATScanLog table in CATSCAN db on dev-sqlsrv1
#     """
#     # create connection string for logging table
#     conn_details_log = "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['Log']
#     conn = pyodbc.connect(conn_details_log)
#     cursor = conn.cursor()
#
#     # unpack data from json
#     int_type, percentLmt, aadt_major, aadt_minor, bca_bool, bca_dict, startDate, endDate = data['IntersectionType'], \
#         data['PercentLimit'], data['AadtMajor'], data['AadtMinor'], data['IncludeBCA'], data['Bca'], data['StartDate'], \
#         data['EndDate']
#
#     # make bca data, crash data, and error message strings
#     bca_dict_str = str(bca_dict)
#     df_log = str(data['Data'])
#     error = str(error)
#
#     # remove extraneous single quotes that would cause SQL errors
#     bca_dict_str = bca_dict_str.replace("'","")
#     df_log = df_log.replace("'","")
#     error_stack = error_stack.replace("'","")
#     error = error.replace("'","")
#
#     # create the SQL INSERT statement
#     log_sql_insert = (f"insert into SegCETLog (clientID, projectID, HighwayClass, error_stack, error, aadt, startDate, endDate, crashData) "
#                       f"values ('{clientID}', '{projectID}', '{int_type}', '{error_stack}', '{error}', '{percentLmt}', '{aadt_major}','{aadt_minor}','{bca_bool}','{bca_dict_str}','{startDate}','{endDate}','{df_log}')")
#
#     cursor.execute(log_sql_insert)
#     cursor.commit()

def cet_seg(data, projectId, clientId):
    conn_details = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['CatScan'])
    conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

    # unpack json data
    hwy_class, startDate, endDate = data['HighwayClass'], data['StartDate'], data['EndDate']
    seg_len, exp_crash_mi_yr, aadt = data['SegmentLength'], data['ExpectedCrashMileYear'], data['Aadt']
    cmfs, full_life, inflation = data['Cmfs'], data['FullServiceLife'], data['Inflation']
    df = pd.DataFrame(data['Data'])
    hwy_class = hwy_class.replace(" ", "_")
    aadt_class = aadt_level(aadt, hwy_class, conn_str)

    # Convert new Crash Manners to Old
    df = conversion(df)
    # Get binary columns for surf cond, lighting, and some crash manners
    df = dummy_wrapper(df)

    # Get cost per crash severity level
    crash_costs = pd.read_sql('CrashPrices', conn_str)['Price'].apply(int).to_list()
    # Get background percents for current hwy class/adt class.
    severity_percents = get_state_percents(aadt_class, hwy_class, conn_str)

    # Temp values for comparing to example CET Excel file
    # crash_costs = [1710561.00, 489446.00, 173578.00, 58636.00, 24982.00]  # TEMP values for validation
    # severity_percents = pd.Series([0.01037037037,0.008148148, 0.060, 0.3059259259, 0.615555556])  # TEMP values for validation

    # Calculate Observed Crashes per Year
    years = int(endDate.split('-')[0]) - int(startDate.split('-')[0]) + 1
    sev_list = ['Fatal','Serious','Minor','Possible','PDO']
    sev_code_list = [100,101,102,103,104]
    sev_dict = dict(zip(sev_list,sev_code_list))
    sev_counts = Counter(df.SeverityCode)
    obs_crashes = {sev: sev_counts[str(code)]/years for sev,code in sev_dict.items()}

    # Calculate Expected Crashes per Year (using value from CAT Scan)
    exp_crashes = exp_crash_mi_yr * seg_len * severity_percents

    # Add severity index to expected crashes and severity percents
    exp_crashes.index = sev_list
    severity_percents.index = sev_list
    # Create Crash Costs dict with severities as keys
    crash_costs_dict = dict(zip(sev_list, crash_costs))

    ref_metrics = [full_life, exp_crashes, severity_percents, crash_costs, inflation]

    # Unpack all values and dynamically create all CMF objects
    cmf_list = [CMF(x, *y.values(), *ref_metrics, df) for x, y in zip(cmfs.keys(), cmfs.values())]
    cmf_dict = [{
        'id': c.id,
        'desc': c.desc,
        'cmf': c.cmf,
        'est_cost': c.est_cost,
        'srv_life': c.srv_life,
        'ben_yr': c.ben_per_year,
        'ben_cost_ratio': c.bc_ratio,
        'exp_srv_life_ben': c.total_benefit,
        'full_cost':c.full_cost} for c in cmf_list]

    # combined cmf results
    combined_cmf = round(prod([c.adj_cmf for c in cmf_list]), 4)
    crash_reduced = round(sum(exp_crashes * (1-combined_cmf)),2)
    total_cost = sum([c.full_cost for c in cmf_list])
    ben_per_year = round(sum([(exp_c-(exp_c*combined_cmf))*cc for exp_c, cc in zip(exp_crashes,crash_costs)]),2)
    total_benefit = round(pv(inflation, full_life, ben_per_year), 2)
    bc_ratio = round(total_benefit/total_cost, 3)

    combined_results = {'comb_cmf': combined_cmf,
                        'crash_reduced': crash_reduced,
                        'total_cost': total_cost,
                        'ben_per_year': ben_per_year,
                        'total_benefit': total_benefit,
                        'bc_ratio': bc_ratio}

    outputDict = {
        'clientId' : clientId,
        'projectId' : projectId,
        'ind_cmfs': cmf_dict,
        'comb_cmf': combined_results,
        'exp_crashes': round(exp_crashes,4).to_dict(),
        'crash_costs':crash_costs_dict,
        'severity_percents':round(severity_percents,3).to_dict(),
        'obs_crashes':obs_crashes
    }

    return json.dumps(outputDict)

def on_request(ch, method, props, body):
    bodyobj = json.loads(body)
    message = bodyobj['message']
    crashdata = json.loads(message)
    projectId = crashdata['ProjectId']
    clientId = bodyobj['clientId']

    status_event = {'ClientId':clientId,'Status': 'Pending', 'Message':'Job was picked up by CET Worker'}
    publish_statusevent(ch, json.dumps(status_event), 'JobStatus')

    try:
        response = cet_seg(crashdata, projectId, clientId)  # Actual CatScan Calculation
        status_event = {'ClientId':clientId,'Status': 'Success', 'Message': 'CET Calculation finished successfully.'}
        publish_statusevent(ch, json.dumps(status_event), 'JobStatus')
        publish_event(ch, method, response, config['RabbitMQ']['DestinationQueue'])

    except Exception as e:
        print(e)
        error_stack = traceback.format_exc()
        print(error_stack)
        response = error_stack
        # log_func(crashdata,clientId,projectId,error_stack,e)
        status_event = {'ClientId':clientId,'Status': 'Failure', 'Message': str(e)}
        publish_event(ch, method, json.dumps(status_event), 'JobStatus')
        # Send response to client of just error message?

def main():
    global config
    config = configparser.ConfigParser()
    config.read('config.ini')
    credentials = pika.PlainCredentials(config['RabbitMQ']['UserName'], config['RabbitMQ']['Password'])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config['RabbitMQ']['Endpoint'],
            virtual_host=config['RabbitMQ']['VirtualHost'],
            client_properties={
                'Machine_Name': 'py-' + str(uuid.uuid4()),
            }
        ))
    channel = connection.channel()
    channel.queue_declare(queue=config['RabbitMQ']['SourceQueue'],durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=config['RabbitMQ']['SourceQueue'], on_message_callback=on_request)

    print(" [x] Awaiting CET Requests...")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
        print(" [x] Awaiting CET Requests...")

        # logging.debug('Startup Successful. Service is now running.')
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
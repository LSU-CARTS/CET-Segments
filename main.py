import sys
import os
import pandas as pd
import numpy as np
import pika
from io import BytesIO
import json
import urllib
import uuid
import configparser
from math import prod
import traceback
import pyodbc
from CMF_class import CMF
from cet_funcs import conversion, dummy_wrapper, get_state_percents

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

def cet_seg(data):
    conn_details = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['CatScan'])
    conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

    # unpack json data
    hwy_class, aadt_class, startDate, endDate = data['Hwy_class'], data['Adt_class'], data['StartDate'], data['EndDate']
    cmfs, full_life = data['Cmfs'], data['Full_life']
    df = pd.DataFrame(data['Data'])

    # get year information
    from_year = int(startDate[0:4])
    to_year = int(endDate[0:4])
    crash_years = to_year - from_year + 1

    severity_percents = get_state_percents(aadt_class, hwy_class, conn_str)

    df = conversion(df)
    df = dummy_wrapper(df)
    total_crashes = len(df.index)
    exp_crashes = total_crashes*severity_percents/crash_years

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
        publish_event(ch, method, response, 'CETSegmentsResults')
        print('here2')
    except Exception as e:
        print(e)
        error_stack = traceback.format_exc()
        # log_func(crashdata,clientId,projectId,error_stack,e)
        status_event = {'ClientId':clientId,'Status': 'Failure', 'Message': str(e)}
        publish_event(ch, method, json.dumps(status_event), 'JobStatus')

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
    channel.queue_declare(queue=config['RabbitMQ']['Queue'],durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=config['RabbitMQ']['Queue'], on_message_callback=on_request)

    print(" [x] Awaiting CET Requests...")
    channel.start_consuming()


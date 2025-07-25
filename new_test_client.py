import pika
import pandas as pd
import json
import configparser
import urllib

# Old example data
# doc_string = "069-02_16-18.xlsx"
# df = pd.read_excel(doc_string, sheet_name='segment - mod')
# df['CrashDate'] = df.CrashDate.astype(str)
# data = df.to_dict(orient='records')  # .to_json(orient='records')

def sent_data_cap(sent_data):
    with open('example io\\sent_data.json', 'w', encoding='utf-8') as f:
        json.dump(sent_data, f, ensure_ascii=False, indent=4)
def return_data_cap(return_data):
    with open('example io\\return_data.json', 'w', encoding='utf-8') as f:
        json.dump(return_data, f, ensure_ascii=False, indent=4)

io_capture_bool = False

global config
config = configparser.ConfigParser()
config.read('config.ini')
conn_details = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};" + config['ConnectionStrings']['CatScan'])
conn_str = f'mssql+pyodbc:///?odbc_connect={conn_details}'

# ===Road Features===
# hwy_class = "Rural_2-Lane"
# aadt = 12500
# aadt_class = aadt_level(aadt,conn_str)

# Start and End date
# start_date = '2016-1-1'
# end_date = '2018-12-31'
#
# full_life_set = 20
# inflation = 0.04
#
# seg_len = 3.012
# exp_crash_per_mile_year = 2.98670702387608

# cmfs = {
#     '4736':
#         {
#             'cmf': 0.825,
#             'desc': 'description of CMF1',
#             'crash_attr': ['All'],
#             'severities': ['All'],
#             'est_cost': 60240,
#             'srv_life': 5
#         },
#     '8101':
#         {
#             'cmf': 0.887,
#             'desc': 'description of CMF2',
#             'crash_attr': ['All'],
#             'severities': ['All'],
#             'est_cost': 66264,
#             'srv_life': 5
#         },
#     '8137':
#         {
#             'cmf': 0.861,
#             'desc': 'description of CMF3. Wet roads',
#             'crash_attr': ['Wet road'],
#             'severities': ['All'],
#             'est_cost': 66264,
#             'srv_life': 5
#         }
# }


# Send data and metrics to CAT Scan
# json_data = {'message':json.dumps({'Data': data, 'HighwayClass': hwy_class,
#                                    'StartDate': start_date, 'EndDate': end_date, 'Cmfs': cmfs, 'FullServiceLife': full_life_set,
#                                    'Aadt': aadt, 'Inflation': inflation, 'SegmentLength': seg_len, 'ExpectedCrashMileYear': exp_crash_per_mile_year,
#                                    'ProjectId': '12345'}), 'clientId':'BCA Test 2-20-2024 #17'}

with open("example io/CQT_example_data.json") as f:
    json_data = json.load(f)

# if io_capture_bool:
#     sent_data_cap(json_data)

# Send jobs to the worker queue
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Prepare and send your job data
channel.basic_publish(exchange='',
                      routing_key=config['RabbitMQ']['SourceQueue'],
                      body=json.dumps(json_data))
print("Job sent")

# Close the channel used for sending jobs
connection.close()

# ===========Listen for results============
result_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
result_channel = result_connection.channel()

# Declare an exchange where the worker posts results
result_channel.exchange_declare(exchange=config['RabbitMQ']['DestinationQueue'], exchange_type='fanout')

# Create a unique queue for this client
result_queue = result_channel.queue_declare(queue='', exclusive=True)
result_queue_name = result_queue.method.queue

# Bind the client's queue to the exchange
result_channel.queue_bind(exchange=config['RabbitMQ']['DestinationQueue'], queue=result_queue_name)

def callback(ch, method, properties, response):
    try:
        # ch.basic_ack()
        print(f"Received result")
        # Process the result here
        response = json.loads(response)
        print(response)
        print('\n')
        ind_cmfs = response['ind_cmfs']
        comb_cmf = response['comb_cmf']
        exp_crashes = response['exp_crashes']
        obs_crashes = response['obs_crashes']
        severity_percents = response['severity_percents']
        crash_costs = response['crash_costs']
        df_resp = pd.DataFrame(ind_cmfs)
        print(df_resp.to_string())
        print('\n')
        print(comb_cmf)
        print('\n')
        print(exp_crashes)
        print('\n')
        print(obs_crashes)
        print('\n')
        print(severity_percents)
        print('\n')
        print(crash_costs)

        if io_capture_bool:
            return_data_cap(response)

        ch.close()
    except Exception as e:
        print(e)
        raise e


result_channel.basic_consume(queue=result_queue_name, on_message_callback=callback, auto_ack=True)
print("Waiting for results. To exit press Ctrl+C")
result_channel.start_consuming()
print('-=-=-=-End-=-=-=-')

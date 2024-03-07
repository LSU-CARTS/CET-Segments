import pika
import uuid
import sys
import pandas as pd
import json

class CatRpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)

        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)


    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, doc_string):
        # read data and put into JSON format
        df = pd.read_excel(doc_string)
        data = df.to_json()
        # f = open('C:\\Users\\malle72\\Downloads\\formattedoutput.json')
        # set to use Logmile or Milepoint
        lm_or_mp = 'lm'

        # Set area type to Rural or Urban
        ru = 'Urban'

        if ru == 'Urban':
            locDev = 0.04
        elif ru == 'Rural':
            locDev = 0.06

        # calculate avg adt (rough estimates here)
        # aadt = int(round(df.ADT.mean(), -2))
        aadt = 32500
        # set begin and end mile values depending on lm_or_mp value
        # mf and mt will be passed from CQT
        if lm_or_mp == 'lm':
            mf = min(df.LogMile)
            mt = max(df.LogMile)
        elif lm_or_mp == 'mp':
            mf = min(df.MilePoint[df.MilePoint > 0])
            mt = max(df.MilePoint)

        mf = 0.0
        mt = 5.66

        # calculating step size and window size
        sigma = (mt - mf) / round((mt - mf) / (max(min((mt - mf) / 100, locDev), locDev / 2)), 0)
        sigma = round(sigma,3)
        delta = round(max(min((mt - mf) / 2, locDev * 15), locDev * 2) / sigma, 0) * sigma

        # calculate percent limit threshold
        percentLimit = max(0.9, (min(0.99, 0.0001 * len(df) + 0.89)))
        percentLimit = round(percentLimit,3)
        print([mf,mt])
        print([sigma, delta, percentLimit])

        # Whether the user wants to do the Benefit-Cost Analysis or not
        bca_bool = False

        # example BCA input
        # Crash Modification Factors; one for each injury level; Must be length 5
        cmfs = [0.5,0.7,0.8,0.9,0.9]

        # costs of suggested counter-measure
        costs = {'const':3000000,
                 'eng':500000,
                 'RoW':450000,
                 'util':50000,
                 'inflat':0.03}

        # expected service life of the suggested counter-measure
        srvLife = 10

        # One dict that contains all user input attributes for BCA
        # first 5 items are CMFs for the different severity levels of crash
        # const -> util are various costs associated with building a countermeasure
        # 'inflat' is an expected inflation figure; set by DOTD policy
        # srvLife is the expected service life of the countermeasure
        bca_dict = {
            'pdo': 0.9,
            'possible': 0.9,
            'minor': 0.8,
            'major': 0.7,
            'fatal': 0.5,
            'const': 3000000,
            'eng': 500000,
            'RoW': 450000,
            'util': 50000,
            'inflat': 0.03,
            'srvLife': 10
        }


        # Send data and metrics to CAT Scan
        self.json_data = {'data': data, 'hwy_class': 'Urban_6-Lane', 'delta': delta, 'sigma': sigma,
                          'percentLimit': percentLimit, 'aadt': aadt, 'lm_or_mp': lm_or_mp, 'mf': mf, 'mt': mt,
                          'bca_bool': bca_bool, 'bca_dict': bca_dict}
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='cat_test_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id),
            body=json.dumps(self.json_data))

        while self.response is None:
            self.connection.process_data_events()
        return self.response

cat_rpc = CatRpcClient()


# --------------------- Sending Request and Post Processing ----------------------------
print(" [x] Requesting CAT Scan")
# response = json.loads(cat_rpc.call("C:/Users/malle72/projects/Copy of Site List.xlsx"))
response = json.loads(cat_rpc.call("013-04_18-20_new.xlsx"))
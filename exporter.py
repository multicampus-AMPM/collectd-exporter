from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from datetime import datetime, timedelta
import re
from threading import Thread
from threading import RLock
from flask import Flask, request
from prometheus_flask_exporter import PrometheusMetrics
import json
import argparse
import os
import requests
import time
import asyncio


WIKI_NAMES = {
    'reallocated-sector-count':'reallocated-sectors-count',
    'power-on-hours':'power-on-hours',
    'power-cycle-count':'power-cycle-count',
    'wear-leveling-count':'wear-range-delta',
    'used-reserved-blocks-total':'used-reserved-block-count-total',
    'program-fail-count-total':'program-fail-count-total',
    'erase-fail-count-total':'erase-fail-count',
    'runtime-bad-block-total':'sata-downshift-error-count',
    'reported-uncorrect':'reported-uncorrectable-errors',
    'airflow-temperature-celsius':'temperature-difference',
    'hardware-ecc-recovered':'hardware-ecc-recovered',
    'udma-crc-error-count':'ultradma-crc-error-count',
    'good-block-rate':'good-block-count-and-system(free)-block-count',
    'total-lbas-written':'total-lbas-written',
    'total-lbas-read':'total-lbas-read'
}


def new_name(vl, idx):
    if vl['plugin'] == vl['type']:
        name = 'collectd_' + vl['type']
    else:
        # case: Type[smart_badsectors, smart_powercycles, smart_poweron, smart_temperature]
        # XXX 빼야할 수도 있음
        if vl['type'] in ['smart_badsectors', 'smart_powercycles', 'smart_poweron', 'smart_temperature']:
            vl['type_instance'] = vl['type'].replace('smart_', '')
            vl['type'] = 'smart_attribute'
            vl['dsnames'][idx] = 'pretty'
        name = 'collectd_' + vl['plugin'] + '_' + vl['type']

    if vl['dsnames'][idx] != 'value':
        # XXX: dsname이 숫자로 안 들어오는데?
        # dsname = vl['dsnames'][idx]
        # if vl['type'] == 'smart_attribute':
        #     if dsname == '0':
        #         print(dsname)
        #         dsname = 'current'
        #     elif dsname == '1':
        #         print(dsname)
        #         dsname = 'worst'
        #     elif dsname == '2':
        #         print(dsname)
        #         dsname = 'threshold'
        #     elif dsname == '3':
        #         print(dsname)
        #         dsname = 'pretty'
        # name += '_' + dsname
        name += '_' + vl['dsnames'][idx]
    if vl['dstypes'][idx] == 'derive' or vl['dstypes'][idx] == 'counter':
        name += '_total'
    
    # modify type instance's value
    # to identify smart attribute's name as in Wikipedia
    # ref : https://en.wikipedia.org/wiki/S.M.A.R.T.
    if vl['type'] == 'smart_attribute':
        wiki_name = WIKI_NAMES.get(vl['type_instance'])
        if wiki_name is not None:
            vl['type_instance'] = wiki_name
    return re.sub(r"[^a-zA-Z0-9_:]", "_", name)


def new_label(vl):
    labels = dict()
    if vl['plugin_instance'] != "":
        labels[vl['plugin']] = vl['plugin_instance']
    if vl['type_instance'] != "":
        if vl['plugin_instance'] == "":
            labels[vl['plugin']] = vl['type_instance']
        else:
            labels["type"] = vl['type_instance']
    labels['instance'] = vl['host']
    return labels


def new_desc(vl, idx):
    return f"Collectd_exporter: '{vl['plugin']}'  Type: '{vl['type']}' Dstype: '{vl['dstypes'][idx]}' Dsname: '{vl['dsnames'][idx]}'"


def new_metric(vl, idx, wrapper):
    name = new_name(vl, idx)
    labels = new_label(vl)
    try:
        metric = wrapper[name]
    except KeyError:
        desc = new_desc(vl, idx)
        dstype = vl['dstypes'][idx]
        if dstype == 'gauge':
            wrapper[name] = GaugeMetricFamily(name=name, documentation=desc, labels=labels.keys())
        elif dstype == 'derive' or dstype == 'counter':
            wrapper[name] = CounterMetricFamily(name=name, documentation=desc, labels=labels.keys())
        metric = wrapper[name]
    metric.add_metric(labels.values(), vl['values'][idx])


def make_identifier(vl):
    vl_id = f"{vl['host']}/{vl['plugin']}"
    if vl['plugin_instance'] != '':
        vl_id += '-' + vl['plugin_instance']
    vl_id += '/' + vl['type']
    if vl['type_instance'] != '':
        vl_id += '-' + vl['type_instance']
    return vl_id


class CollectdExporter(object):
    """ Convertor data from collectd to metrics which prometheus supports """

    timeout = 2

    def __init__(self, collector):
        self.collector = collector

    def collect(self):
        value_lists = self.collector.get_value_lists()
        wrapper = dict()
        now = datetime.now()
        for vl_id in value_lists:
            vl = value_lists[vl_id]
            time = datetime.fromtimestamp(float(vl['time']))
            valid_until = time + timedelta(seconds=(CollectdExporter.timeout * vl['interval']))
            if valid_until < now :
                continue
            for idx in range(len(vl['values'])):
                new_metric(vl, idx, wrapper)
        for metric in wrapper:
            yield wrapper[metric]


@DeprecationWarning
async def call_predictor():
    """ call the smart-predictor asynchronously """
    max = 3
    ticker = 1
    print(f'[INFO] calling predictor after {max} seconds')
    # 이게 최초에 항상 실행이 되네? 왜지
    # metrics 긁어갈떄마다 요청하는거 확인함 근데 왜 이게 최초에 실행되는지 모르겠네
    # request만 최초에 실행하고 resoponse도 print 안하고 있음
    while ticker <= max:
        print(f'[INFO] ticking...{ticker}')
        time.sleep(ticker)
        ticker += 1
    
    response = requests.get(url=os.environ['addr'], params={'time': time.time(), 'model': 'smart-model', 'query' : ['collectd_smart_smart_attribute_pretty', 'collectd_smart_smart_attribute_current']}, timeout=10)
    print(response)


class CollectdCollector(Thread):
    """ Thread to hold data from collectd """

    def __init__(self, group=None, target=None, name=None, args=None, *kwargs, daemon=None):
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self.value_lists = dict()
        self.lock = RLock()

    def run(self):
        print(" * CollectdCollector Daemon Starts.")

    def set_value_lists(self, value_list):
        with self.lock:
            # TODO : 시간 지난값 value_lists에서 삭제 필요한지 확인
            for vl in value_list:
                id = make_identifier(vl)
                self.value_lists[id] = vl

    def get_value_lists(self):
        with self.lock:
            value_lists = self.value_lists.copy()
        return value_lists


app = Flask(__name__)
exporter = PrometheusMetrics(app)
collector = CollectdCollector(daemon=True)


@app.route('/favicon.ico')
@exporter.do_not_track()
def favicon():
    return 'ok'


@app.route('/')
@exporter.do_not_track()
def main():
    """ context root """
    return """
        <html>
            <head><title>Collectd Exporter</title></head>
            <body>
                <h1>Collectd Exporter</h1>
                <p><a href='/metrics'>Metrics</a></p>
            </body>
        </html>
    """


@app.route('/collectd', methods=['POST'])
@exporter.do_not_track()
def collectd_post():
    """ endpoint for collectd """
    data = request.data
    try:
        value_list = json.loads(data)
    except: 
        app.logger.error("InternalServerError : invalid data from collectd")
        return 'ok'
    collector.set_value_lists(value_list)
    return 'ok'


@DeprecationWarning
def parse_config():
    """ cofing parser for command line arguments
        --host : the service host of this app (default: 0.0.0.0)
        --port : the service port of this app (default: 9103)
        --addr : the address of predictor (default: None) """
    parser = argparse.ArgumentParser(description='collectd_exporter configuration')
    parser.add_argument('--host', metavar='host', type=str, help='the service host')
    parser.add_argument('--port', metavar='port', type=int, help='the service port')
    parser.add_argument('--addr', metavar='addr', type=str, help='the address of predictor')
    args = parser.parse_args()

    
    if args.host is None:
        args.host = '0.0.0.0'
    if args.port is None:
        args.port = 9103

    return args


def parse_env():
    # use get function instead of key reference to avoid error
    host = os.environ.get('host')
    port = os.environ.get('port')
    addr = os.environ.get('addr')

    # TODO regex validation
    if host is None:
        os.environ['host'] = '0.0.0.0'
    if port is None:
        os.environ['port'] = '9103'
    if addr is None:
        os.environ['addr'] = 'http://smart-predictor:9106/predict'


if __name__ == '__main__':
    # args = parse_config()
    parse_env()
    collector.start()
    exporter.registry.register(CollectdExporter(collector))
    app.run(host=os.environ.get('host'), port=os.environ.get('port'))

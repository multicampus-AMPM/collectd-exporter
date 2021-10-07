#!/usr/bin/env python3

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


def new_name(vl, idx):
    if vl['plugin'] == vl['type']:
        name = 'collectd_' + vl['type']
    else:
        name = 'collectd_' + vl['plugin'] + '_' + vl['type']
    if vl['dsnames'][idx] != 'value':
        name += '_' + vl['dsnames'][idx]
    if vl['dstypes'][idx] == 'derive' or vl['dstypes'][idx] == 'counter':
        name += '_total'
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
    global collector
    collector.set_value_lists(value_list)
    return 'ok'


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


if __name__ == '__main__':
    # args = parse_config()
    parse_env()
    collector.start()
    exporter.registry.register(CollectdExporter(collector))
    app.run(host=os.environ.get('host'), port=os.environ.get('port'))

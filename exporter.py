#!/usr/bin/env python3

from flask import Flask, request
from prometheus_flask_exporter import PrometheusMetrics
import json
from collector import CollectdExporter, CollectdCollector


app = Flask(__name__)
exporter = PrometheusMetrics(app)
collector = CollectdCollector(daemon=True)


@app.route('/favicon.ico')
@exporter.do_not_track()
def favicon():
    pass


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
    if data is None:
        # TODO logger.error(InternalServerError)
        return "not ok"
    value_list = json.loads(data)
    global collector
    collector.set_value_lists(value_list)
    return 'ok'


if __name__ == '__main__':
    collector.start()
    exporter.registry.register(CollectdExporter(collector))
    # TODO: 설정 파일 파싱 (host, port, 예측 모듈 주소)
    app.run(host='0.0.0.0', port=9105)

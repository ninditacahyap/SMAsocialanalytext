from flask import Flask, redirect, url_for, render_template, render_template_string
import time
from redis import Redis
from rq import Queue
from rq.job import Job

app = Flask(__name__)

r = Redis(host='redisserver')
q = Queue(connection=r)

def slow_function(data):
    time.sleep(5)
    return 'Processed %s' % (data,)


template_str='''<html>
    <head>
      {% if refresh %}
        <meta http-equiv="refresh" content="5">
      {% endif %}
    </head>
    <body>{{result}}</body>
    </html>'''

def get_template(data, refresh=False):
    return render_template_string(template_str, result = data, refresh=refresh)

@app.route('/process/<string:data>')
def process(data):
    job = q.enqueue(slow_function, data)
    return redirect(url_for('result', id=job.id))

@app.route('/result/<string:data>')
def result(id):
    job = Job.fetch(id, connection=r)
    status = job.get_status()
    if status in ['queued', 'started', 'deferred', 'failed']:
        return get_template(status, refresh=True)
    elif status == 'finished':
        result = job.result
        return get_template(result)
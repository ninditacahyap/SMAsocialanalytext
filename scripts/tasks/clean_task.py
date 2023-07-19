from redis import Redis
import rq
from ..cleaning import Clean
from flask import redirect, url_for
import sys


def fast_clean(target, from_date, to_date):
    clean = Clean(target, from_date, to_date)
    clean.clean_auto()

def clean_rq(target, from_date, to_date, depend=""):
    queue = rq.Queue('sm-worker', connection=Redis.from_url('redis://'))
    if depend != "":
        job = queue.enqueue(fast_clean, args=[target, from_date, to_date], depends_on=depend, job_timeout='10m')
    else:
        job = queue.enqueue(fast_clean, args=[target, from_date, to_date], job_timeout="3m")
    return queue, job
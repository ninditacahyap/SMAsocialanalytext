from redis import Redis
import rq
from ..cleaning import Clean
from flask import redirect, url_for
import sys


def fast_clean(target):
    clean = Clean(target)
    clean.clean_auto()

def clean_rq(target, depend=""):
    queue = rq.Queue('sm-worker', connection=Redis.from_url('redis://'))
    if depend != "":
        job = queue.enqueue(fast_clean, args=[target], depends_on=depend, job_timeout='3m')
    else:
        job = queue.enqueue(fast_clean, args=[target], job_timeout="3m")
    return queue, job
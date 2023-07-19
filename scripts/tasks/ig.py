from redis import Redis
import rq
from rq.job import Job
from flask import redirect, url_for, flash
from ..instascraper import InstaScrapper
import sys

sys.path.append('..')

def fetch_ig(instagram, from_date, to_date):
  scrapper= InstaScrapper()
  
  try:
    print('Login...')
    scrapper.login()
    print('Login succeed')
  except:
    print('Login Error!')
    return redirect(url_for('index'))

  try:
    _ = scrapper.scrape_all(instagram, from_date, to_date)
  except:
    print('Error on Scrapping Progress!')
    return redirect(url_for('index'))

def get_fetch_job(instagram, from_date, to_date):
  queue = rq.Queue('sm-worker', connection=Redis.from_url('redis://'))
  job = queue.enqueue('scripts.tasks.ig.fetch_ig', args=[instagram, from_date, to_date], job_timeout='1h')
  return queue, job
from flask import Flask
from flask import render_template
from flask import redirect
from flask import url_for
from flask import request
from flask import render_template_string
from flask import flash
from flask import session
from scripts.cleaning import Clean
from scripts import instascraper
from scripts import topics
from scripts.tasks.ig import get_fetch_job
from scripts.tasks.clean_task import clean_rq
from scripts.tasks.topics_task import topic_task
from scripts.tasks.analytics_task import analytic_rq
from redis import Redis
import rq
from rq.job import Job
import time
import glob

r = Redis.from_url('redis://')

app = Flask(__name__)
app.secret_key = "super secret key"

@app.route('/', methods=["GET"])
def index():
    if request.method == 'GET':
        scrapping_url = url_for('scrapping')
        return render_template('index.html', scrapping_url=scrapping_url)

@app.route('/scrapping', methods=['GET', 'POST'])
def scrapping():
    if request.method == 'GET':
        return render_template('dataScrapping.html')

    if request.method == 'POST':
        instagram = request.form.get('instagram')
        # dari = request.form.get('dari').split('/')
        # hingga = request.form.get('hingga').split('/')
        # from_date = [int(dari[2]), int(dari[0]), int(dari[1])]
        # to_date = [int(hingga[2]), int(hingga[0]), int(hingga[1])]

        # _, job = get_fetch_job(instagram, from_date, to_date)
        # _, clean_job = clean_rq(target=instagram, depend=job)
        # return redirect(url_for('cleaning', instagram=instagram, index=0, id=clean_job.id, refresh=False))
        return redirect(url_for('process_clean', instagram=instagram))

@app.route('/process-clean/<string:instagram>')
def process_clean(instagram):
    _, job = clean_rq(instagram)
    return redirect(url_for('cleaning', index=0, instagram=instagram, id=job.id))

# @app.route('/clean/<string:instagram>', methods=['GET', 'POST'])
# def clean(instagram):
#     clean = Clean(instagram)
#     topic_url = url_for('process_topic', instagram=instagram)
#     if request.method == 'GET':
#         wordcloud_images = url_for('static', filename= f'images/{instagram}/wordcloud_{instagram}.png', instagram=instagram)
#         return render_template('dataCleaning.html', wordcloud=wordcloud_images, topic_url=topic_url)

#     if request.method == 'POST':
#         hapus = request.form.get('hapus')
#         hapus = str(hapus)
#         list_hapus = hapus.replace(' ', '').split(',')
#         clean.clean_manual(list_hapus)
#         wordcloud_images = url_for('static', filename= f'images/{instagram}/wordcloud_{instagram}.png', instagram=instagram)
#         return redirect(url_for('clean', instagram=instagram, wordcloud=wordcloud_images))

@app.route('/cleaning/<string:instagram>/<string:id>/<int:index>', methods=['GET', 'POST'])
def cleaning(instagram, id, index):
    clean = Clean(instagram)
    topic_url = url_for('process_topic', instagram=instagram)
    if request.method == 'GET':
        job = Job.fetch(id, connection=r)
        status = job.get_status()
        if status in ['queued', 'started', 'deferred']:
            return render_template('dataCleaning.html', index=index, refresh=True, instagram=instagram, topic_url=topic_url)
        elif status == 'failed':
            flash("Error on fetching instagram data. Please wait for a moment...")
            return render_template('index.html')
        elif status == 'finished':
            wordcloud_images = url_for('static', filename= f'images/{instagram}/wordcloud_{instagram}.png', instagram=instagram)
            return render_template('dataCleaning.html', index=index, refresh=False, wordcloud=wordcloud_images, topic_url=topic_url)

    if request.method == 'POST':
        hapus = request.form.get('hapus')
        hapus = str(hapus)
        list_hapus = hapus.replace(' ', '').split(',')
        clean.clean_manual(list_hapus)
        wordcloud_images = url_for('static', filename= f'images/{instagram}/wordcloud_{instagram}.png', instagram=instagram)
        return redirect(url_for('cleaning', index=index+1, instagram=instagram, id=id, refresh=False, wordcloud=wordcloud_images))

@app.route('/process-topic/<string:instagram>')
def process_topic(instagram):
    _, job = topic_task(instagram)
    return redirect(url_for('topics', instagram=instagram, id=job.id))

@app.route('/topics/<string:instagram>/<string:id>', methods=['GET', 'POST'])
def topics(instagram, id):
    cohorence_images = url_for('static', filename=f'images/{instagram}/coherence_{instagram}.png')
    if request.method == 'GET':
        job = Job.fetch(id, connection=r)
        status = job.get_status()
        if status in ['queued', 'started', 'deferred']:
            return render_template('topicDiscovery.html', refresh=True,  cohorence_images=cohorence_images)
        elif status == 'failed':
            flash("Error on getting topics graph, Please wait for a moment...")
            return render_template('index.html')
        elif status == 'finished':
            top_coherence = job.result
            return render_template('topicDiscovery.html', refresh=False, cohorence_images=cohorence_images, top_coherence=top_coherence)
    
    if request.method == 'POST':
        num_topic = request.form.get('topik')
        return redirect(url_for('analytics_task', instagram=instagram, n_topics=num_topic))

@app.route('/process-analytics/<string:instagram>/<string:n_topics>')
def analytics_task(instagram, n_topics):
    n_topic = int(n_topics)
    _, job = analytic_rq(instagram, n_topic)
    return redirect(url_for('analytics', instagram=instagram, n_topics=n_topics, id=job.id))

@app.route('/analytics/<string:instagram>/<string:n_topics>/<string:id>')
def analytics(instagram, n_topics, id):
    topic_cloud = url_for('static', filename=f'images/{instagram}/topic_cloud_{instagram}.png')
    if request.method == 'GET':
        job = Job.fetch(id, connection=r)
        status = job.get_status()
        if status in ['queued', 'started', 'deferred']:
            return render_template('contentAnalysis.html', refresh=True, instagram=instagram)
        elif status == 'failed':
            flash('Error on getting analytics result. Please wait a moment...')
            return render_template('index.html')
        elif status == 'finished':
            scripts, div, topic_cloud_list, topic_colors_list = job.result
            kwargs = {'scripts': scripts,
            'ept': div['ept'],
            'cph': div['cph'],
            'cpt': div['cpt'],
            'p': div['p'],
            'cpd': div['cpd']}
            return render_template('contentAnalysis.html', refresh=False, instagram=instagram, topic_cloud_list=topic_cloud_list, topic_colors_list=topic_colors_list, **kwargs)

if __name__ == '__main__':
    app.run()
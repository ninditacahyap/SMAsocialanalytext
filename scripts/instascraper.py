from nltk.corpus import stopwords
import nltk
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
import pytesseract
import cv2
import instaloader
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from itertools import dropwhile, takewhile
import threading
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
import time


from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
factory = StemmerFactory()
stemmer = factory.create_stemmer()
factory = StopWordRemoverFactory()
stopword = factory.create_stop_word_remover()


nltk.download('stopwords')

accounts = [
  ['daslim_virgo12', '123098abc'],
  ['jejakproses', 'Berprosesbertumbuh'],
  ['cobacoba7320', 'cobacoba0'],
  ['buatsmanalytics', 'cobacoba1']
]

'''
# proxy
proxy_username = 'cadizsw191199'
proxy_password = '7A656F08C40BA7F8D905CFBE60B77253'
hostname = '5.181.40.75:12333'

proxies = {
    "http": "http://{0}:{1}@{2}/".format(proxy_username, proxy_password, hostname),
    "https": "https://{0}:{1}@{2}/".format(proxy_username, proxy_password, hostname)
  }
'''


class InstaScrapper():
  final_df = {}

  def __init__(self, download_videos=False, post_metadata="{likes};{comments};{url};[{caption}]"):
    self.L = instaloader.Instaloader(
        download_videos=download_videos, post_metadata_txt_pattern=post_metadata)
    self.CURRENT_DIR = os.getcwd()

  # instagram username and password
  
  def login(self):
    # username = username
    # password = password
    for acc in accounts:
      try:
        self.L.login(acc[0], acc[1])
        break
      except instaloader.exceptions.TooManyRequestsException:
        print('login failed')
        continue

  def get_current_df_date(self, target, since, until):
    try:
      df = pd.read_csv(target)
      df['Date'] = pd.to_datetime(df['Date'])

      filtered_df = df[(df['Date'] >= since) & (df['Date'] <= until)]
      date_list = filtered_df['Date'].to_list()
      date_list = [x.to_pydatetime() for x in date_list]
      return date_list
    except:
      return []


  def fetch_ig(self,target, since=None, until=None):
    self.target = target
    self.target_dir = os.path.join(os.path.join(self.CURRENT_DIR, 'instagram'), target)
    self.L.dirname_pattern = self.target_dir
    posts = instaloader.Profile.from_username(self.L.context, target).get_posts()
    threads = []
    if since != None and until != None:
      SINCE = datetime(since[0], since[1], since[2])
      UNTIL = datetime(until[0], until[1], until[2])
      if SINCE < UNTIL:
        SINCE, UNTIL = UNTIL, SINCE
      print('here')
      
      filtered_dates = self.get_current_df_date(os.path.join(self.target_dir, f"{self.target}.csv"), f"{since[0]}-{since[1]}-{since[2]}", f"{until[0]}-{until[1]}-{until[2]}")

      for post in takewhile(lambda p: p.date > UNTIL, dropwhile(lambda p: p.date > SINCE, posts)):
        print("qqq",post.date)
        if post.date in filtered_dates:
          print(f"{post.date} are already exist")
          continue
        else: 
          fmt_date = (str(post.date)).replace(' ','_').replace(':','-')
          self.final_df[fmt_date] = []

          try:
            self.L.download_post(post, target)
            target_txt = glob.glob(f"{self.target_dir}/{fmt_date}*UTC.txt")
            target_pic = glob.glob(f"{self.target_dir}/{fmt_date}*.jpg")
            x = threading.Thread(target=self.get_likes_comments_df, args=(target_txt,fmt_date,))
            y = threading.Thread(target=self.get_images_text_df, args=(target_pic,fmt_date,))
            threads.append(x)
            threads.append(y)
            x.start()
            y.start()
          except:
            print("Error occured")
    else: 
      for post in posts:
        print("zzz",post.date)
        self.L.download_post(post, target)
    for index, thread in enumerate(threads):
        thread.join()
        print("Main    : thread {} done".format(index))
    
  def get_likes_comments_df(self,target_txt,fmt_date):
    namafiles = [i[len(self.target_dir)+1 :] for i in target_txt]
    dates_df = pd.DataFrame({
        "Date": namafiles
    })
    dates_df["Date"] = dates_df["Date"].map(lambda x: x[:19].replace("-","").replace("_",""))
    dates_df["Date"] = pd.to_datetime(dates_df["Date"], format="%Y%m%d%H%M%S")

    files = [pd.read_csv(file, delimiter=';', names =['likes', 'comments','url','caption'] ) for file in target_txt]

    files_df = pd.concat(files) 
    files_df = files_df.reset_index().drop("index",axis=1)

    df = pd.concat([dates_df, files_df], axis=1, join="inner")
    print("\nnice",target_txt)
    print(df)
    self.final_df[fmt_date].append(df)

  def get_images_text(self,target_pic):
    text = []

    for pic in sorted(target_pic):
      img = cv2.imread(pic)
      img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
      extracted_text = pytesseract.image_to_string(img)
      extracted_text = " ".join(extracted_text.replace("\n", " ").split())

      text.append(extracted_text)
    return text

  def get_steps(self,target_pict):
    steps = []
    target_pic = [i[len(self.target_dir)+1:] for i in sorted(target_pict)]
    txt = target_pic[0][:19]  
    j = 0

    for pic in target_pic:
      if pic[:19] == txt:
        j = j + 1
      else:
        steps.append(j)
        txt = pic[:19]
        j = 1
    if target_pic[-1][-5] == "C":
      steps.append(1)
    else:  
      steps.append(int(target_pic[-1][-5]))
    return steps

  def get_images_text_df(self,target_pic,fmt_date):
    extracted_text = self.get_images_text(target_pic)
    steps = self.get_steps(target_pic)

    text_array = np.empty((len(steps),10), dtype="object")

    d = 0
    for i in range(text_array.shape[0]):
      for j in range(steps[i]):
        text_array[i, j] = extracted_text[d]
        d = d+ 1
    
    cols_text = ["text{}".format(i) for i in range(1, 11)]

    df = pd.DataFrame(text_array, columns=cols_text)
    self.final_df[fmt_date].append(df)

  def scrape_all(self, target, since=None, until=None):
    self.fetch_ig(target, since, until)
    res = []
    for i in self.final_df:
      res.append(pd.concat(self.final_df[i], axis=1, join="inner"))
    if len(res) != 0:
      final_df = pd.concat(res)
      final_df['final'] = final_df['text1'].astype(str) + " " + final_df['text2'].astype(str) + " "  + final_df['text3'].astype(str) + " "  + final_df['text4'].astype(str) + " "  + final_df['text5'].astype(str) + " "  + final_df['text6'].astype(str) + " "  + final_df['text7'].astype(str) + " "  + final_df['text8'].astype(str) + " "  + final_df['text9'].astype(str) + " "  + final_df['caption'].astype(str)
      final_df = final_df[['Date', 'final', 'likes', 'comments', 'url']]
      try:
        cur_df = pd.read_csv(os.path.join(self.target_dir, f"{self.target}.csv"))
        merged_df = cur_df.merge(final_df, on='Date')
        merged_df.to_csv(os.path.join(self.target_dir, f"{self.target}.csv"))
      except:
        final_df.to_csv(os.path.join(self.target_dir, f"{self.target}.csv"))
    else:
      try:
        final_df = pd.read_csv(os.path.join(self.target_dir, f"{self.target}.csv"))
      except :
        print('final_df have no value.')
        print('It could because there are no post fetched or because the filtered date range have no post')
        print('make sure you filter valid dates')    
    return final_df

  def scrape(self):
    akun = input('Enter target username: ')
    akhir_tahun = int(input('Enter tahun akhir: '))
    akhir_bulan = int(input('Enter bulan akhir: '))
    akhir_tanggal = int(input('Enter tanggal akhir: '))
    awal_tahun = int(input('Enter tahun awal: '))
    awal_bulan = int(input('Enter bulan awal: '))
    awal_tanggal = int(input('Enter tanggal awal: '))
    start = time.time()
    final_df = self.scrape_all(akun, (akhir_tahun,akhir_bulan,akhir_tanggal), (awal_tahun,awal_bulan,awal_tanggal))
    end = time.time()
    final_df['final'] = final_df['text1'].astype(str) + " " + final_df['text2'].astype(str) + " "  + final_df['text3'].astype(str) + " "  + final_df['text4'].astype(str) + " "  + final_df['text5'].astype(str) + " "  + final_df['text6'].astype(str) + " "  + final_df['text7'].astype(str) + " "  + final_df['text8'].astype(str) + " "  + final_df['text9'].astype(str) + " "  + final_df['caption'].astype(str)
    res = final_df[['Date','final','likes','comments','url']]
    return res

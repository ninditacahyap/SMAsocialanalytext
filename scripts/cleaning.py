from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
from flask import session

#from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
#factory = StemmerFactory()
#stemmer = factory.create_stemmer()
#from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
#factory = StopWordRemoverFactory()
#stopword = factory.create_stop_word_remover()

import os

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

static_dir = os.path.join(os.getcwd(), 'static')
images_dir = os.path.join(static_dir, 'images')
instagram_dir = os.path.join(os.getcwd(), 'instagram')

class Clean():
  def __init__(self, target):
    self.target_dir = os.path.join(instagram_dir, target)
    self.target = target
    self.target_images_dir = os.path.join(images_dir, target)
    self.arg = pd.read_csv(os.path.join(self.target_dir, f"{target}.csv"))

  def character(self):
    self.arg['final']=self.arg['final'].str.replace('(?:\@|https?\://)\S+', '')
    self.arg['final']=self.arg['final'].str.replace('[^\w\s]',' ')
    #ilangin angka
    self.arg['final']=self.arg['final'].str.replace('\d+',' ')
    #ilangin enter
    self.arg['final']=self.arg['final'].str.lower()
    #ilangin spasi berlebih
    self.arg['final'] = self.arg['final'].replace('\s+', ' ', regex=True)
    #ilangin simbol
    self.arg['final'] = self.arg['final'].replace('\n',' ',regex=True)
    self.arg['final'] = self.arg['final'].str.split().map(lambda sl: " ".join(s for s in sl if len(s) > 1))
    text = " ".join(i for i in self.arg.final)
    #wordcloud = WordCloud().generate(text)
    #Display the generated image
    #plt.figure(figsize=(12, 9), dpi=80)
    #plt.imshow(wordcloud, interpolation="bilinear")
    return self.arg
  def slang(self):
    stop = stopwords.words('english')
    self.arg['final'] = self.arg['final'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
    stop = stopwords.words('indonesian')
    self.arg['final'] = self.arg['final'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
    text1 = " ".join(i for i in self.arg.final)
    #wordcloud = WordCloud().generate(text1)
    #Display the generated image
    #plt.figure(figsize=(12, 9), dpi=80)
    #plt.imshow(wordcloud, interpolation="bilinear")
    return self.arg
  def clean_manual(self, list):
    lists = list
    def remove_freqwords(arg):
        """custom function to remove the frequent words"""
        return " ".join([word for word in str(arg).split(' ') if word not in lists])
    #self.arg["final"] = self.arg["final"].apply(lambda arg: remove_freqwords(arg))
    #text = " ".join(i for i in self.arg.final)
    #wordcloud = WordCloud().generate(text)
    #Display the generated image
    #plt.figure(figsize=(12, 9), dpi=80)
    #plt.imshow(wordcloud, interpolation="bilinear")
    try:
      final_df = pd.read_csv(os.path.join(self.target_dir, f"{self.target}_final.csv"))
    except:
      final_df = self.arg
    final_df["final"] = final_df["final"].apply(remove_freqwords)
    final_df = final_df[['Date', 'final', 'likes', 'comments', 'url']]
    final_df.to_csv(os.path.join(self.target_dir, f"{self.target}_final.csv"), index=False)
    final = ' '.join(i for i in final_df.final)
    self.generate_cloud_words(final)
    return final_df
  
  def clean_auto(self):
    self.arg = self.character()
    self.arg = self.slang()
    self.arg[['likes','comments']] = self.arg[['likes','comments']].astype('int')
    self.arg = self.arg[['Date','final','likes','comments','url']]
    self.arg.to_csv(os.path.join(self.target_dir, f"{self.target}.csv"), index=False)
    self.arg.to_csv(os.path.join(self.target_dir, f"{self.target}_final.csv"), index=False)
    self.generate_cloud_words()
    return self.arg

  def generate_cloud_words(self, target=""):
    try:
      os.makedirs(os.path.join(images_dir, self.target))
    except:
      pass
    
    if (target == ""):
      text = ' '.join(i for i in self.arg.final)
    else:
      text = target
    print(text)
    wordcloud = WordCloud(width=1600, height=800, background_color='white').generate(text)
    plt.figure(figsize=(20,10), facecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout(pad=0)
    plt.savefig(os.path.join(self.target_images_dir, f"wordcloud_{self.target}.png"), facecolor='k', bbox_inches='tight')


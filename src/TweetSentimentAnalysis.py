import sys
import json
import re
import textblob as txt
from textblob import Word
import nltk
from nltk.corpus import stopwords
import datetime as datetime
from src.data.MongoDB import MongoDB
from src.business.Common import Common
import os.path as path

Common.Configuration = Common.read_appconfig()
log_file = Common.Configuration['log_file']

default_language = ['en', 'english']
stopw_language = {  'en' : 'English',
                    'es' : 'Spanish',
                    'pt' : 'Portuguese',
                    'de' : 'German',
                    'fr' : 'French',
                    'it' : 'Italian'}

punctuation = set({'.', ',', ':', ';', '/', '?', '}', ']', '[', '{', '(', ')', '+', '=', '-', '_', '§', '!', '@', '#', '$', '%', '&', '*', '\\', '|' })
valid_char = '[^a-zA-Z0-9@#áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ ]'

StageLimit = 500 # int(sys.stdin.readline())

#connect to Stage database
Database = MongoDB('DB_DATABASE_STAGE_NAME')

#select documents without sentiment analysis
docList = Database.Find({'polarity': None}, Common.Configuration['Database']['DB_TABLE_TWEET_DATA'], StageLimit)

i=0
for flowFile in docList:

    #remove URL and invalid characters
    text = flowFile['text']
    text = re.sub(r'\S*https?:\S*', ' ', text)
    text = re.sub(valid_char, ' ', text)

    # textblob analysis
    ##
    ## criar metodo para registrar log no mongodb
    ##
    try:
        phrase = txt.TextBlob(text) 
    except Exception as ex: 
        f = open(log_file, 'a')
        f.write('\n\nAnalysis error: ' + flowFile['id'] + ';')
        f.close

    # stop words from original and english languages 
    stop_origin  = set()
    if flowFile['lang'] in ['en','es','pt','de','fr','it']:
        stop_origin  = set(stopwords.words(stopw_language[flowFile['lang']].lower()))
    stop_english = set(stopwords.words(default_language[1]))

    token_origin = set([tko[0] for tko in phrase.pos_tags]) 
    token_origin = token_origin - stop_origin - punctuation

    # translate from original language to english
    translated = text
    if flowFile['lang'] != default_language[0]:
        try:
            t = phrase.translate(from_lang=flowFile['lang'], to=default_language[0])
            translated = str(t)
            phrase = txt.TextBlob(translated)
        except Exception as ex:
            f = open(log_file, 'a')
            f.write('\n\nTranslation error: ' + flowFile['id'] + ';')
            f.close


    token_english = set([tko[0] for tko in phrase.pos_tags])
    token_english = token_english - stop_english - punctuation

    # NiFi flowfile enrichment
    flowFile['cleaner_text']  = text
    flowFile['translated']    = translated
    flowFile['polarity']      = phrase.sentiment.polarity
    flowFile['subjectivity']  = phrase.sentiment.subjectivity
    flowFile['token_text']    = list(token_origin)
    flowFile['token_english'] = list(token_english)
    flowFile['update_date']   = datetime.datetime.now()

    ## update the document with sentiment polarity, subjectivity, and translated text
    try:
        updated = Database.UpdateItems(Filter={'$and': [{'_id': flowFile['_id']}, {'polarity': None}]},
                                     Update={'$set': flowFile},
                                     Collection=Common.Configuration['Database']['DB_TABLE_TWEET_DATA'],
                                     Upsert=True)
    except Exception as ex:
        f = open(log_file, 'a')
        f.write('\n\nUpdate error: ' + flowFile['id'] + ';')
        f.close
    i=i+1
print('Sentiment Analysis has executed. Total:  ' + str(i))


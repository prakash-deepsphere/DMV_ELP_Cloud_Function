"""
© Copyright 2022, California, Department of Motor Vehicle, all rights reserved.
The source code and all its associated artifacts belong to the California Department of Motor Vehicle (CA, DMV), and no one has any ownership
and control over this source code and its belongings. Any attempt to copy the source code or repurpose the source code and lead to criminal
prosecution. Don't hesitate to contact DMV for further information on this copyright statement.

Release Notes and Development Platform:
The source code was developed on the Google Cloud platform using Google Cloud Functions serverless computing architecture. The Cloud
Functions gen 2 version automatically deploys the cloud function on Google Cloud Run as a service under the same name as the Cloud
Functions. The initial version of this code was created to quickly demonstrate the role of MLOps in the ELP process and to create an MVP. Later,
this code will be optimized, and Python OOP concepts will be introduced to increase the code reusability and efficiency.
____________________________________________________________________________________________________________
Development Platform                | Developer       | Reviewer   | Release  | Version  | Date
____________________________________|_________________|____________|__________|__________|__________________
Google Cloud Serverless Computing   | DMV Consultant  | Ajay Gupta | Initial  | 1.0      | 09/18/2022

"""

# Importing Libraries

import pandas as pd
import numpy as np
import itertools
import re

import nltk
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
nltk.download('stopwords')

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from tensorflow.keras.layers import Embedding
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import Sequential
from tensorflow.keras.preprocessing.text import one_hot



class PreprocessTextClassification:
    def __init__(self,data,target_column):
        self.data = data
        self.target_column = target_column
    def data_preprocessing(self,vAR_test_data):
        print('*'*30+'DATA PRE-PROCESSING'+'*'*30+'\n\t\t1.Remove Stop Words\n\t\t2.Stemming/Lemmatization')
        vAR_ps = PorterStemmer()
        vAR_corpus = []
        if vAR_test_data is None:
            data = self.data
        else:
            data = vAR_test_data
        for i in range(0, len(data)):
            vAR_review = re.sub('[^a-zA-Z]', ' ', data['comment_text'][i])
            vAR_review = vAR_review.lower()
            vAR_review = vAR_review.split()

            vAR_review = [vAR_ps.stem(word) for word in vAR_review if not word in stopwords.words('english')]
            vAR_review = ' '.join(vAR_review)
            vAR_corpus.append(vAR_review)
        return vAR_corpus
    def bagofwords_vectorization(self,vAR_corpus,vAR_test_data):
        vAR_cv = CountVectorizer(max_features=5000,ngram_range=(1,3))
        vAR_X = vAR_cv.fit_transform(vAR_corpus).toarray()
        if vAR_test_data is None:
            vAR_y = self.data[self.target_column]
        else: 
            vAR_y = vAR_test_data[self.target_column]
        return vAR_X,vAR_y
    def tfidf_vectorization(self):
        pass
    def word_embedding_vectorization(self,vAR_corpus,vAR_test_data):
        vAR_voc_size=10000
        vAR_sent_length=8
        vAR_onehot_repr=[one_hot(words,vAR_voc_size)for words in vAR_corpus]
        vAR_embedded_docs=pad_sequences(vAR_onehot_repr,padding='pre',maxlen=vAR_sent_length)
        vAR_model=Sequential()
        vAR_model.add(Embedding(vAR_voc_size,10,input_length=vAR_sent_length))
        vAR_model.compile('adam','mse')
        vAR_X = vAR_model.predict(vAR_embedded_docs)
        if vAR_test_data is None:
            vAR_y = self.data[self.target_column]
        else: 
            vAR_y = vAR_test_data[self.target_column]
        return vAR_X,vAR_y
        
        

    
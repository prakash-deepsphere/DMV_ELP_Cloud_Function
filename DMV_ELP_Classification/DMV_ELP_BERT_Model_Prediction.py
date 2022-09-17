import pandas as pd
import numpy as np
import tensorflow as tf

from transformers import TFBertModel,  BertConfig, BertTokenizerFast, TFAutoModel
from random import randint
import time
import json
import gcsfs
import h5py
import traceback

def BERT_Model_Result(vAR_input_text):
    
    
    
    vAR_test_sentence = vAR_input_text
    vAR_target_columns = ['Toxic','Severe Toxic','Obscene','Threat','Insult','Identity Hate']
    
    # Name of the BERT model to use
    model_name = 'bert-base-uncased'

    # Max length of tokens
    max_length = 128

    # Load transformers config and set output_hidden_states to False
    config = BertConfig.from_pretrained(model_name)
    #config.output_hidden_states = False

    # Load BERT tokenizer
    tokenizer = BertTokenizerFast.from_pretrained(pretrained_model_name_or_path = model_name, config = config)
    
    vAR_test_x = tokenizer(
    text=vAR_test_sentence,
    add_special_tokens=True,
    max_length=max_length,
    truncation=True,
    padding=True, 
    return_tensors='tf',
    return_token_type_ids = False,
    return_attention_mask = True,
    verbose = True)
    start_time = time.time()
    # print('Copying Model')
    # subprocess.call(["gsutil cp gs://dsai_saved_models/BERT/model.h5 /tmp/"],shell=True)
    # print('Model File successfully copied')  
    MODEL_PATH = 'gs://dmv_elp_project/saved_model/BERT/model.h5'
    # MODEL_PATH = 'gs://dsai_saved_models/BERT/BERT_MODEL_64B_4e5LR_3E'
    FS = gcsfs.GCSFileSystem()
    with FS.open(MODEL_PATH, 'rb') as model_file:
         model_gcs = h5py.File(model_file, 'r')
         vAR_load_model = tf.keras.models.load_model(model_gcs,compile=False)
    # vAR_load_model = tf.keras.models.load_model('gs://dsai_saved_models/BERT/model.h5',compile=False)
    # vAR_load_model = tf.keras.models.load_model(MODEL_PATH,compile=False)
    # vAR_load_model = tf.keras.models.load_model('/tmp/model.h5',compile=False)

    # vAR_load_model = Load_BERT_Model()
    
    print("---Bert Model loading time %s seconds ---" % (time.time() - start_time))
    

    vAR_model_result = vAR_load_model.predict(x={'input_ids': vAR_test_x['input_ids'], 'attention_mask': vAR_test_x['attention_mask']},batch_size=32)
    
    # if "vAR_load_model" not in st.session_state:
    #     st.session_state.vAR_load_model = tf.keras.models.load_model('DSAI_Model_Implementation_Sourcecode/BERT_MODEL_64B_4e5LR_3E')
    # vAR_model_result = st.session_state.vAR_load_model.predict(x={'input_ids': vAR_test_x['input_ids'], 'attention_mask': vAR_test_x['attention_mask']},batch_size=32)
    vAR_result_data = pd.DataFrame(vAR_model_result,columns=vAR_target_columns)
    vAR_target_sum = (np.sum(vAR_model_result)*100).round(2)
    vAR_result_data.index = pd.Index(['Percentage'],name='category')
    vAR_result_data = vAR_result_data.astype(float).round(5)*100
    
    if vAR_target_sum>20:
        return False,vAR_result_data,vAR_target_sum
    else:
        return True,vAR_result_data,vAR_target_sum

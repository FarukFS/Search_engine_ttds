from flask import Flask, make_response, request, render_template
from traceback import print_exc
from flask_cors import CORS
from controller.utils.mogo_utils import get_mongo_conn
# from controller.utils.ProcessQueryRAM import process_query, get_lyrics
from controller.utils.ProcessQueryWithPhrase import process_query, get_lyrics
from nltk.stem import PorterStemmer
from os import environ
from logging import getLogger
import json
import pandas as pd
import pickle
from apiclient.discovery import build

logger = getLogger()

flask_app = Flask(__name__)
CORS(flask_app,allow_headers='content_type',origins='*')

@flask_app.before_first_request
def load_global_data():
    global ps, conn, STwords, db, inv_index, collection,  col_len, col_nolyrics, terms_index, bm_avg, docs, api_object
        #init stemmer
    ps = PorterStemmer()
    #load stopwords
    f = open('./controller/static/englishST.txt', "r", encoding="utf-8-sig")
    STwords = [word.rstrip() for word in f.readlines()]
    #mongo conn
    conn = get_mongo_conn(host=environ["MONGO_HOST"], port=int(environ["MONGO_PORT"]))
    db = conn["indexes"]

    with open('./controller/static/index.pickle', 'rb') as handle:
        inv_index = pickle.load(handle)
    with open('./controller/static/terms_index.pickle', 'rb') as handle:
        terms_index = pickle.load(handle)
    with open('./controller/static/bm_avg.pickle', 'rb') as handle:
        bm_avg = pickle.load(handle)

    collection = db["collection"]
    with open('./controller/static/collection_nolyrics.pickle', 'rb') as handle:
        col_nolyrics = pickle.load(handle)
    col_len = collection.count_documents({})
    docs = set(range((col_len)))
    api_object = build('youtube', 'v3', developerKey='AIzaSyACy9B-4tLJtoudsIiz--r_kzA9_j6UI0U')


@flask_app.route("/")
def front():
    return render_template("./ttds.html")


@flask_app.route("/search", methods=["POST"])
def search():
    payload = request.get_json(force=True)
    global ps, conn, STwords, db, inv_index, col_nolyrics,  col_len, terms_index, bm_avg, docs
    type = payload["type"].lower() 
    query = payload["query"]
    # num_results = payload["num_results"]
    logger.info("[INFO] Executing query")
    try:
        if type =="boolean" or type=="ranked" or type=='ranked_bm':
            
            response = process_query(query=query, qtype=type, col_len=col_len, 
                        collection=col_nolyrics, inv_index=inv_index, ps=ps, STwords=STwords, bm_avg=bm_avg, bm_index=terms_index, docs=docs)
            return make_response(response, 200)
        else:
            response={"body": "query not supported"}
            
            return make_response(response,500)  
        
        
    except Exception as e:
        print_exc()
        response = {'error':str(e)}
        return make_response(response, 400)
    
    
@flask_app.route("/lyrics", methods=["POST"])
def lyrics():
    payload = request.get_json(force=True)
    logger.info("[INFO] Executing query")
    global ps, conn, STwords, db, inv_index, collection, col_len
    try:
        response = get_lyrics(payload, collection)
        return make_response(response, 200)
        
    except Exception as e:
        print_exc()
        response = {'error':str(e)}
        return make_response(response, 400)

@flask_app.route("/get_song", methods=["POST"])
def get_song():
    payload = request.get_json(force=True)
    logger.info("[INFO] Executing query")
    global api_object
    try:
        response = get_song(payload, api_object)
        return make_response(response, 200)

    except Exception as e:
        print_exc()
        response = {'error': str(e)}
        return make_response(response, 400)

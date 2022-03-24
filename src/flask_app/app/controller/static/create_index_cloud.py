from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
import pickle
from os import environ
from pymongo import MongoClient
import os
import argparse


def get_mongo_conn(host='127.0.0.1', port=8949):
    # Create connection
    mongo_conn = MongoClient(host, port)
    return mongo_conn


def create_invindex(file_csv):
    """
    Given a collection of documents, creates an inverted index.
    :param file: a file containing the collection of documents.
    :return: an inverted index and a list containing the docIDs (used later for ranked search).
    """
    # Read list of stopwords and initialize porter stemmer.
    f = open('englishST.txt', "r", encoding="utf-8-sig")
    STwords = [word.rstrip() for word in f.readlines()]
    ps = PorterStemmer()
    # Parse XML file and initialize data structures.
    inv_index = defaultdict(list)
    terms_index = {}
    bm_avg = 0

    df = pd.read_csv(file_csv)
    df = df.dropna(subset=['Lyric'])
    lyrics_list = df['Lyric'].tolist()
    del df
    # Iterate through all of the documents in the collection
    for idx, lyric in enumerate(lyrics_list):
        print("*" * 5, idx)
        # Combine headline + text, tokenize it, remove the stopwords and stem each token.
        tks = re.findall(r'\w+', lyric)
        tks = [ps.stem(word) for word in tks if word.lower() not in STwords]
        bm_avg += len(tks)
        # Iterate through each token and append to inverted index.
        for pos, word in enumerate(tks):
            # Check if the word is already on the inv_index
            if word in inv_index:
                # If the word already ocurred in the document, append new position to the position list.
                if idx in inv_index[word][1]:
                    inv_index[word][1][idx].append(pos)
                # Else, create position list for that document and append the single position.
                else:
                    inv_index[word][1][idx] = [pos]

            else:
                # Create placeholder for document frequency of word.
                inv_index[word].append('')
                # Declare posting list for new word.
                inv_index[word].append({})
                # For each word, add document ID : [pos] pair.
                inv_index[word][1][idx] = [pos]

            # Calculate document frequency of word.
            inv_index[word][0] = len(inv_index[word][1].keys())

        terms_index[idx] = len(tks)

    bm_avg = bm_avg/len(inv_index)

    with open('index.pickle', 'wb') as handle:
        pickle.dump(inv_index, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('terms_index.pickle', 'wb') as handle:
        pickle.dump(terms_index, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('bm_avg.pickle', 'wb') as handle:
        pickle.dump(bm_avg, handle, protocol=pickle.HIGHEST_PROTOCOL)


def upload_index(index, connection):
    with open(index, 'r') as f:
        i_index = json.load(f)
    db = connection["indexes"]
    inv_index = db["inv_index"]

    for key, val in i_index.items():
        testeo = {"Token": key, 'Frequency': val[0], 'Documents': val[1]}
        inv_index.insert_one(testeo)


def upload_collection(collection, connection):
    with open(collection, 'r') as f:
        df = pd.read_csv(f)
    db = connection["indexes"]
    collec = db["collection"]
    collec_nolyrics = {}
    for i in range(len(df)):
        print("*" * 5, i)
        example = df.iloc[i][{'Lyric', 'Artist', 'SName', 'ID', 'Genres', 'Release', 'Thumbnail'}].to_dict()
        collec.insert_one({x: (int(y) if x == "ID" else str(y)) for x, y in example.items()})

        example_nol = df.iloc[i][{'Artist', 'SName', 'ID', 'Genres', 'Release', 'Thumbnail'}].to_dict()
        collec_nolyrics[int(df.iloc[i]['ID'])] = example_nol

    with open('collection_nolyrics.pickle', 'wb') as handle:
        pickle.dump(collec_nolyrics, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', type=str, required=True)
    args = parser.parse_args()

    connection = get_mongo_conn()
    upload_collection(args.collection, connection=connection)

    create_invindex(args.collection)
    #upload_index(os.getcwd()+'/index.json', connection)

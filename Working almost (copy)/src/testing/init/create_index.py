from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
from os import environ
from pymongo import MongoClient


def get_mongo_conn(host='127.0.0.1', port=27017):
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
    df = pd.read_csv(file_csv)
    df = df.dropna(subset=['Lyric'])
    lyrics_list = df['Lyric'].tolist()

    # Iterate through all of the documents in the collection
    for idx, lyric in enumerate(lyrics_list):
        # Combine headline + text, tokenize it, remove the stopwords and stem each token.
        tks = re.findall(r'\w+', lyric)
        tks = [ps.stem(word) for word in tks if word.lower() not in STwords]
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

    with open('./init/index.json', 'w+') as f:
        json.dump(inv_index, f)


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

    df = df.dropna(subset=['Lyric'])
    df.rename(columns={'Unnamed: 0': 'ID'}, inplace=True)
    df['Genres'] = 0
    df['Release'] = 0
    df['Thumbnail'] = 0
    df.loc[df['Artist'] == '10000 Maniacs', 'Artist'] = '10,000 Maniacs'

    for i in range(len(df)):
        print("*"*5, i)
        example = df.iloc[i][{'Lyric', 'Artist', 'SName','ID', 'Genres', 'Release', 'Thumbnail'}].to_dict()
        collec.insert_one({x:(int(y) if x=="ID" else str(y)) for x,y in example.items()})

if __name__=="__main__":
    connection = get_mongo_conn()
    upload_collection('./init/dataset.csv', connection=connection)

    create_invindex('dataset.csv')
    upload_index('./init/index.json', connection)

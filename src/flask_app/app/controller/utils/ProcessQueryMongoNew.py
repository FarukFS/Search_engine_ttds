from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
import time
from pymongo import MongoClient


def get_mongo_conn(host='127.0.0.1', port=27017):
    # Create connection
    mongo_conn = MongoClient(host, port)
    return mongo_conn


# connection = get_mongo_conn()
# db = connection["indexes"]
# inv_index = db["inv_index"]
# collection = db["collection"]
#
# col_len = collection.count_documents({})

# f = open('englishST.txt', "r", encoding="utf-8-sig")
# STwords = [word.rstrip() for word in f.readlines()]
# ps = PorterStemmer()


def get_word(inv_index, word):
    """
    Find a word in the inverted index and return the list of documents for that word.
    :param inv_index: the inverted index.
    :param word: a word.
    :return: list of documents containing the word.
    """
    return inv_index.find_one({'Token':word})


def proximity_search(inv_index, word1, word2, distance=1):
    """
    Given two words, perform a proximity search.
    :param inv_index: the inverted index.
    :param word1: a word.
    :param word2: a word.
    :param distance: the distance between the words.
    :return: list of documents containing the word.
    """
    a = get_word(inv_index, word1)['Documents']
    b = get_word(inv_index, word2)['Documents']
    # Documents that contain both word a and word b.
    match = a.keys() & b.keys()
    newdict = {k: (a[k], b[k]) for k in match}
    documents = set()
    # For each position of word a, iterate through all the positions of word b and store the result if the distance requirement is satisfied.
    for key, val in newdict.items():
        for i in range(len(val) - 1):
            for j in range(len(val[i])):
                temp = val[i][j]
                for k in range(len(val[i + 1])):
                    temp2 = val[i + 1][k]
                    if distance == 1:  # Phrase search
                        if temp2 - temp == distance:
                            documents.add(key)
                    else:  # Proximity search
                        if abs(temp2 - temp) <= distance:
                            documents.add(key)

    documents = sorted(list(map(int, documents)))
    return documents


def boolean(inv_index, query, ps, STwords):
    query = re.findall(r'\w+', query.lower())
    query = ' '.join([ps.stem(word) for word in query if word not in STwords])
    query = [word for word in re.split(" (and|or) ", query)]
    result = set()
    operator = None
    words = []
    i = -1

    if " ".join(query).find(" ") == -1:
        tmp = get_word(inv_index, query[0])['Documents'].keys()
        return list(map(int, tmp))

    for word in query:
        not_flag = False

        if word in ['and', 'or']:
            operator = word
            continue

        if word.find("not ") == 0:
            not_flag = True
            word = word[4:]
            words.append(word)
            i += 1

        else:
            words.append(word)
            i += 1

        if operator:
            if len(words) == 2:
                result = set(get_word(inv_index, words[i - 1])['Documents'].keys())
            temp = set(get_word(inv_index, words[i])['Documents'].keys())

            if operator == 'and':
                if not_flag == True:
                    result -= temp
                else:
                    result &= temp

            elif operator == 'or':
                if not_flag == True:
                    result = result
                else:
                    result |= temp

        operator = None

    return list(map(int, result))


def ranked_search(query, inv_index, col_len, ps, STwords):
    """
    Given a ranked query, perform a ranked search.
    :param query: a query.
    :param inv_index: the inverted index.
    :param docs: a list containing all of the document IDs.
    :return: sorted list of tuples containing the score for each document in the format (doc, score).
    """

    queries = re.findall(r'\w+', query.lower())
    queries = [ps.stem(word) for word in queries if ps.stem(word) not in STwords]

    query_index = {}
    tfidf_index = defaultdict(dict)

    for word in queries:
        query_index[word] = 1 + math.log(queries.count(word), 10)
        docs = get_word(inv_index, word)['Documents']
        freq = get_word(inv_index, word)['Frequency']
        for doc in docs.keys():
            tfidf_index[doc][word] = (1 + math.log(len(docs[doc]), 10)) * (math.log((col_len / freq), 10))

    scores = []
    for key in tfidf_index.keys():
        score = 0
        for word in queries:
            if tfidf_index.get(key, {}).get(word) is not None:
                score += query_index[word] * (tfidf_index[key][word])
        if score > 0:
            scores.append((key, score))

    return sorted(scores, key=lambda tup: tup[1], reverse=True)


def process_query(query, qtype, col_len, collection, inv_index, ps, STwords):
    if qtype == 'boolean':
        results = boolean(inv_index=inv_index, query=query, ps=ps, STwords=STwords)[:10]
        songs = [collection.find_one({'ID':i}) for i in results]
        songs = {'Results': [{'Artist': s['Artist'], 'Title': s['SName'], 'Genres':s['Genres'], 'Release':s['Release'],
                              'Thumbnail':s['Thumbnail']} for s in songs]}
    elif qtype == 'ranked':
        results = ranked_search(query, inv_index=inv_index, col_len=col_len, ps=ps, STwords=STwords)[:10]
        songs = [int(res[0]) for res in results]
        songs = [collection.find_one({'ID':i}) for i in songs[:10]]
#         songs = collection.find({'ID': {"$in": songs}})
        songs = {'Results': [{'Artist': s['Artist'], 'Title': s['SName'], 'Genres':s['Genres'], 'Release':s['Release'],
                              'Thumbnail':s['Thumbnail']} for s in songs]}

    return songs


def get_lyrics(dic, collection):
    lyrics = collection.find_one({'Artist': dic['Artist'], 'SName': dic['Title']})
    return {'Lyrics': lyrics['Lyric']}

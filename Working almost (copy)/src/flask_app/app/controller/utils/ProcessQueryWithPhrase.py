from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
import time
import shlex
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
    if word in inv_index.keys():
        try:
            return set(inv_index[word][1].keys())
        except IndexError:
            return set()
    elif word[0] == "\"" and word[-1] == "\"":
        return handleQuotes(inv_index, word[1:-1])
    #         quotes
    elif word[0] == "#" and word[-1] == ")":
        return proximity_search(inv_index, word)
    else:
        return set()


def get_word_ranked(inv_index, word):
    if word in inv_index.keys():
        try:
            return inv_index[word]
        except IndexError:
            return set()
    elif word[0] == "\"" and word[-1] == "\"":
        return handleQuotes(inv_index, word[1:-1])
    #         quotes
    elif word[0] == "#" and word[-1] == ")":
        return proximity_search(inv_index, word)
    else:
        return set()


# gets both docs and their freqs
def get_word_docs(inv_index, word):
    if word in inv_index.keys():
        try:
            return inv_index[word][1]
        except IndexError:
            return set()
    return set()


def handleQuotes(inv_index, quote, ps, STwords):
    quote = quote.lower()
    quote = quote.split(" ")
    quoteParts = [ps.stem(w) for w in quote if w not in STwords]
    if len(quoteParts) == 1:
        return get_word(inv_index, quoteParts[0])

    results = None
    for word in quoteParts:
        if results == None:
            try:
                results = inv_index[word][1]
            except IndexError:
                return set()
        elif len(results) == 0:
            return set()
        else:
            try:
                newResults = inv_index[word][1]
            except IndexError:
                return set()

            newResults = {k: v for (k, v) in newResults.items() if k in results.keys()}

            keys = list(newResults.keys())
            for doc in keys:
                rd = results[doc]
                nd = newResults[doc]
                nd = [v for v in nd if v - 1 in rd]
                if nd == []:
                    newResults.pop(doc)
                else:
                    newResults[doc] = nd
            results = newResults

    return set(results.keys())


def proximity_search(inv_index, query):
    query = re.findall(r'#\((\w+),(\w+)(?:,(\d+))?', query)
    query = list(filter(None, query))
    a = get_word_docs(inv_index, query[0][0])
    b = get_word_docs(inv_index, query[0][1])
    try:
        distance = int(query[0][2])
    except (IndexError, ValueError):
        distance = 1

    match = a.keys() & b.keys()
    newdict = {k: (a[k], b[k]) for k in match}
    documents = set()
    for key, val in newdict.items():
        for i in range(len(val) - 1):
            for j in range(len(val[i])):
                temp = val[i][j]
                for k in range(len(val[i + 1])):
                    temp2 = val[i + 1][k]
                    if distance == 1:
                        if temp2 - temp == distance:
                            documents.add(key)
                    else:
                        if abs(temp2 - temp) <= distance:
                            documents.add(key)

    return documents


def boolean(inv_index, query, ps, STwords, docs):
    query = query.lower()
    ops = ["and", "or"]
    queryParts = list(map(lambda x: x if x in ops else ps.stem(x), re.split(" (and|or) ", query)))
    queryParts = [w for w in queryParts if (w not in STwords or w in ops)]
    queryParts = list(map(lambda x: x if x in ops else x.lower(), queryParts))
    result = set()
    operator = None
    words = []
    haveNot = []
    i = -1

    if len(queryParts) == 0:
        return set()
    elif len(queryParts) == 1:
        if queryParts[0][:3] == "not":
            return docs - get_word(inv_index, queryParts[0])
        else:
            return get_word(inv_index, queryParts[0])

    for part in queryParts:
        not_flag = False
        if part in ['and', 'or']:
            operator = part
            not_flag = False
            continue

        if part[:3] == "not":
            # print("not found")
            haveNot += [i + 1]
            #     not_flag = i
            word = part[4:]
            words.append(word)
            i += 1
        else:
            words.append(part)
            i += 1

        if operator:
            if len(result) == 0:
                #                 we build results first
                w1Result = get_word(inv_index, words[i - 1])
                # second word results

                if i - 1 in haveNot:
                    w1Result = docs - w1Result

                w2Result = get_word(inv_index, words[i])

                if i in haveNot:
                    w2Result = docs - w2Result

                if operator == 'and':
                    result = w1Result & w2Result
                elif operator == "or":
                    result = w1Result | w2Result
            else:
                print(i)
                # we already have old results - we append
                newResults = get_word(inv_index, words[i])

                if i in haveNot:
                    newResults = docs - newResults

                if operator == 'and':
                    result = result & newResults
                elif operator == "or":
                    result = result | newResults

            operator = None

    return sorted(map(int, result))


def ranked_search(query, inv_index, col_len, ps, STwords, bm_index, bm_avg):
    """
    Given a ranked query, perform a ranked search.
    :param query: a query.
    :param inv_index: the inverted index.
    :param docs: a list containing all of the document IDs.
    :return: sorted list of tuples containing the score for each document in the format (doc, score).
    """

    queries = [ps.stem(word) for word in shlex.split(query, posix=False) if word not in STwords]

    query_index = {}
    tfidf_index = defaultdict(dict)

    for word in queries:
        query_index[word] = 1
        try:
            docs = get_word_ranked(inv_index, word)[1].keys()
            freq = get_word_ranked(inv_index, word)[0]
        except (IndexError, TypeError):
            docs = get_word_ranked(inv_index, word)
            print(docs)
            freq = len(get_word_ranked(inv_index, word))

        for doc in docs:
            tfidf_index[doc][word] = (1 * (1.5 + 1) / (1 + 1.5 * (1 - 0.1 + 0.1 * bm_index[doc] / bm_avg))) * \
                                     (math.log(((col_len - freq + 0.5) / (freq + 0.5)), 10))

    scores = []
    for key in tfidf_index.keys():
        score = 0
        for word in queries:
            if tfidf_index.get(key, {}).get(word) is not None:
                score += query_index[word] * (tfidf_index[key][word])
        if score > 0:
            scores.append((key, score))

    return sorted(scores, key=lambda tup: tup[1], reverse=True)


def process_query(query, qtype, col_len, collection, inv_index, bm_index, bm_avg, STwords, ps, docs, num_results=20):

    if qtype == 'boolean':
        results = boolean(inv_index=inv_index, query=query, STwords=STwords, ps=ps, docs=docs)
        try:
            results = results[: num_results]
        except IndexError:
            results = results
        songs = [collection[i] for i in results]
        songs = {'Results': [{'Artist': s['Artist'], 'Title': s['SName'], 'Genres':s['Genres'], 'Release':s['Release'],
                              'Thumbnail':s['Thumbnail']} for s in songs]}
    elif qtype == 'ranked':
        results = ranked_search(query, inv_index=inv_index, col_len=col_len, STwords=STwords, ps=ps, bm_index=bm_index, bm_avg=bm_avg)[:num_results]
        try:
            results = results[: num_results]
        except IndexError:
            results = results
        songs = [int(res[0]) for res in results]
        songs = [collection[i] for i in songs]
        songs = {'Results': [{'Artist': s['Artist'], 'Title': s['SName'], 'Genres':s['Genres'], 'Release':s['Release'],
                              'Thumbnail':s['Thumbnail']} for s in songs]}
    elif qtype == 'ranked_bm':
        results = ranked_search(query, inv_index=inv_index, col_len=col_len, bm_index=bm_index, bm_avg=bm_avg, STwords=STwords, ps=ps)[:num_results]
        try:
            results = results[: num_results]
        except IndexError:
            results = results
        songs = [int(res[0]) for res in results]
        songs = [collection[i] for i in songs]
        songs = {'Results': [{'Artist': s['Artist'], 'Title': s['SName'], 'Genres':s['Genres'], 'Release':s['Release'],
                              'Thumbnail':s['Thumbnail']} for s in songs]}

    return songs


def get_lyrics(dic, collection):
    lyrics = collection.find_one({'Artist': dic['Artist'], 'SName': dic['Title']})
    return {'Lyrics': lyrics['Lyric']}
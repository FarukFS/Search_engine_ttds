from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
from pymongo import MongoClient
import pickle
import argparse


def get_mongo_conn(host='127.0.0.1', port=27017):
    # Create connection
    mongo_conn = MongoClient(host, port)
    return mongo_conn


# takes list of numbers and transforms them into a list of ints - note that the docID is the first int in nums
def encodeNums(nums):
    #     print(nums)
    # return encoding of a list of nums
    def encode(num):
        #         num += 1
        def power_two(n):
            return int(math.log(n, 2))

        # y = math.pow(2, power_two(num))
        def gamma(num):
            num = num + 1
            #             num = num
            if num == 1:
                return str(format(1, 'b'))
            #             num += 1
            #             if num == 0:
            #                 return "1"
            s = ""
            s += "1" * power_two(num) + "0"
            s += str(format(num - 2 ** power_two(num), 'b')).zfill((power_two(num)))
            return s

        return gamma(num)

    encoding = []
    for num in nums:
        encodedNum = encode(num)
        if encoding == []:
            encoding += [encodedNum]
        else:
            lastNum = encoding[-1]
            # bigger than max bit size allowed, we need to use another element to represent.
            if len(lastNum) + len(encodedNum) >= 32:
                encoding += [encodedNum]
            else:
                encoding[-1] = encoding[-1] + (encodedNum)
    #     print("encoding" , encoding)
    return list(map(lambda x: int(x, 2), encoding))


# "amstwom": [2, {"197936": [69], "197961": [67]}],
# so taking value[1] - {"197936": [69], "197961": [67]}
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

    for kp in inv_index.items():
        # print("kp", kp)
        key = kp[0]
        body = kp[1]
        encodedDocs = []

        docOffset = -1
        lastDocID = -1
        for doc in body[1].items():
            #         set first docID, then rest are related to it
            if docOffset == -1:
                docOffset = int(doc[0])
            else:
                docOffset = int(doc[0]) - lastDocID
            lastDocID = int(doc[0])
            encodedDocs += [encodeNums([docOffset] + doc[1])]
        # replace with encoded. e.g. "fire"
        inv_index[key] = [body[0], encodedDocs]

    with open('index_elias.pickle', 'wb') as handle:
        pickle.dump(inv_index, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('terms_index.pickle', 'wb') as handle:
        pickle.dump(terms_index, handle, protocol=pickle.HIGHEST_PROTOCOL)


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

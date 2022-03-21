from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
import math
from nltk.stem import PorterStemmer
import re
from collections import defaultdict
import math
import pandas as pd
import json
from os import environ
from pymongo import MongoClient



# from pymongo import MongoClient


def get_mongo_conn(host='127.0.0.1', port=27017):
    # Create connection
    mongo_conn = MongoClient(host, port)
    return mongo_conn


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
    df = pd.read_csv(file_csv)
    print(df)
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
    print(inv_index)
    return inv_index

    # with open('index.json', 'w+') as f:
    #     json.dump(inv_index, f)


def upload_index(index, connection):
    with open(index, 'r') as f:
        i_index = json.load(f)

    db = connection["indexes"]
    inv_index = db["inv_index"]

    for key, val in i_index.items():
        testeo = {"Token": key, 'Frequency': val[0], 'Documents': val[1]}
        inv_index.insert_one(testeo)


# connection = get_mongo_conn()
inv_index_orig = create_invindex('dataset.csv')
# upload_index('index.json', connection)




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
                return str(format(1 , 'b'))
#             num += 1
#             if num == 0:
#                 return "1"
            s = ""
            s += "1" *power_two(num) + "0"
            s += str(format(num- 2**power_two(num), 'b')).zfill((power_two(num)))
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





# spits out x... from encodeNums([x...])]) - Of form [numInDoc, [positions]]
# encoding is the entire index for a given word, e.g. "feel" has [[encodedDoc1], [EncodedDoc2]...]
def decode(encoding):
    # ans = {}
#     print(encoding[:10])
#     print("encoding", encoding)
    # nDocs = encoding[0]
    for item in encoding[1][0:]:
        print("doc", item)

#         print(doc[0])
#         for item in doc[0]:
        # print(decode2(doc))
        # item = item
        key = -1
        values = []
        print(item)
        for encodedPart in item:
            # print("encodedPart", encodedPart)
    #         print(bin(encodedPart))
            binNum = str(bin(encodedPart))[2:]
            
            while len(binNum) > 0:
            # y1+z1 = original first number
                try:
                    first0 = binNum.index("0")
                    
                    y1 = binNum[:first0]
                    y1 = 2** len(y1)
                    z1 = binNum[first0:2 * first0+1]
                    
                    val = y1+int(z1, 2) - 1
                    if key == -1:
                        key = val
                    else:
                        values += [val]


                    binNum = binNum[2*first0+1:]
                    
                except ValueError:
    #                 because we +1 zero index, need to plus one again
    #                 no 0's - only 1
                    if key == -1:
                        key = 0
                    else:
                        values += [0]
                    binNum = binNum[1:]
        # currently outputting a single doc and it's values.
        return "docid", key ," pos", values
#     return key, values

    



def createEliasIndex(inv_index_orig):
    newIndex = {}
    print(inv_index_orig)
    for kp in inv_index_orig.items():
        #print("kp", kp)
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
        newIndex[key] = [body[0], encodedDocs]
    with open('elias.json', 'w+') as f:
        json.dump(newIndex, f)
    return newIndex
    
    
def upload_index(index, connection):
    with open(index, 'r') as f:
        i_index = json.load(f)

    db = connection["indexes"]
    inv_index = db["inv_index"]

    for key, val in i_index.items():
        # print("key", key, val)
        testeo = {"Token": key, 'Frequency': val[0], 'Documents': val[1]}
        inv_index.insert_one(testeo)    
    





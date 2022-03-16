from pymongo import MongoClient


def get_mongo_conn(host='127.0.0.1', port=27017):
    # Create connection
    mongo_conn = MongoClient(host, port)
    return mongo_conn
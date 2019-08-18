#!/usr/bin/python3

import configparser
import pprint
import sys

from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('mongoConf.ini')

src = config['PRIME']
dst = config['SECONDARY']


def check_db(client, db):
    if not db in client.list_database_names():
        sys.exit("On host: '{}' the DB: '{}' does not exist!".format(client.address, db))


def client_no_uri(node):
    return MongoClient(host=node.get('host'),
                       port=node.getint('port'),
                       username=node.get('username'),
                       password=node.get('password'),
                       authSource=node.get('authSource'),
                       authMechanism=node.get('authMechanism'))


def client_uri(node):
    client = "mongodb://{}:{}@{}:{}/?authSource={}&authMechanism=SCRAM-SHA-1".format(node.get('username'),
                                                                                     node.get('password'),
                                                                                     node.get('host'),
                                                                                     node.get('port'),
                                                                                     node.get('authSource'))
    return MongoClient(client)


src_client = client_no_uri(src)
check_db(src_client, src.get('src_db'))
src_db = src_client[src.get('src_db')]
collections = src_db.list_collection_names()

dst_client = client_uri(dst)
dst_db = dst_client[dst.get('dst_db')]

for collection in collections:
    src_col = src_db[collection]
    src_list = []
    for documents in src_col.find({}, {'_id': False}):
        src_list.append(documents)
    pprint.pprint(src_list)
    dst_col_ext = src_col.name + dst.get('collection_ext')
    dst_col = dst_db[dst_col_ext]
    dst_col.insert_many(src_list)

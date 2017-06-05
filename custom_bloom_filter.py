# Author: R Santosh
# How to Execute(2.7.13): python custom_bloom.py
# Objective: Fast look up of the swift objects in the cluster
#
# References : https://brilliant.org/wiki/bloom-filter/
#
# 1.Reading each file the Logs folder and collection the Swift Object
# 2.Calculating the md5 digest of the object and then
# 3.Storing/Inserting the object's md5 digest in the Bloom Filter.(Uses Murmur hash function to fill the bit array)
# 4.The object is searched using the lookup method of the bloom filter.

import os
from hashlib import md5
import mmh3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

es = Elasticsearch()
mapping={"mappings":{"objects":{"properties":{"item":{"type":"string","index":"not_analyzed"},
                                              "filename":{"type": "string", "index": "not_analyzed"},
                                              "lineno": {"type":"integer"}
                                              }}}}

def get_logs():
    """ This is just a mock, no need to make it complex"""
    list_of_files = []
    for dirname, subdir, filenames in os.walk("Logs"):
        for filename in os.listdir(dirname):
            if filename.endswith(".txt"):
                abs_path = "%s/%s" % (dirname, filename)
                list_of_files.append(abs_path)
    return list_of_files



def read_in_chunks(file_object, chunk_size=65536):
    while True:
        data = file_object.readlines(chunk_size)
        if not data:
            break
        yield data

def filter_logs(logs):
    """ A generator that yields a es formatted dictionary for matching lines
    :param logs: A list of log files to process
    """
    fd = open(logs)
    index = 0
    offset = 0
    line_no = 0

    for chunk in read_in_chunks(fd):
        for ln in chunk:
            line_no += 1
            try:
                verb, obj, _, status = ln.split()[8:12]
            except ValueError:
                continue
            if verb in ("DELETE", "PUT"):
                yield md5((obj[4:]).encode('utf-8')).hexdigest(), logs, line_no

        offset = index + len(chunk)  # increasing the offset
        index = offset

def lookup(string, bit_array, hash_count, size):
    for seed in range(hash_count):
        result = mmh3.hash(string, seed) % size
        if bit_array[result] == 0:
            return False
    return True

def calculate_md5code(test_obj):
    return md5(test_obj.encode('utf-8')).hexdigest()

if __name__ == "__main__":

    log_files = get_logs()
    es.indices.create(index='bloom', body=json.dumps(mapping))
    count =0

    for item_file in log_files:
        docs = []
        all_objects = filter_logs(item_file)
        for item, filename, lineno in all_objects:
             count +=1
             actions ={
                     "_index": "bloom",
                     "_type": "objects",
                     "_id": count,
                     "_source": {
                         "item": item,
                         "filename": str(filename),
                         "lineno": int(lineno)}
                 }
             docs.append(actions)
        helpers.bulk(es, docs)

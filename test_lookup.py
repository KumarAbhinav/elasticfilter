# Author: R Santosh
# How to Execute(2.7.13): python test_lookup.py objName1 objName2
# Objective: Fast look up of the swift objects in the cluster

from custom_bloom_filter import calculate_md5code
from elasticsearch import Elasticsearch
import json
from flask_restful import Resource, request

es = Elasticsearch()

class FetchDetails(Resource):

    def post(self):
        list_of_objects = []
        objects = request.form['objects']
        objects = objects.split(',')
        for obj in objects:
            list_of_objects.append((calculate_md5code(str(obj))))
        query = {"query": {"terms": {"item": list_of_objects}}}
        result = es.search(index='bloom', doc_type='objects', body=json.dumps(query))
        for data in result["hits"]["hits"]:
            return {"filename": data["_source"]["filename"], "lineno": data["_source"]["lineno"]}



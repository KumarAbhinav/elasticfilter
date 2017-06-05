from flask_restful import Api
from flask import Flask
import test_lookup

app = Flask(__name__)
api = Api(app)

api.add_resource(test_lookup.FetchDetails, "/v1/search/logs")

if __name__ == '__main__':
    app.run()

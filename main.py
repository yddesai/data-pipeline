import time
from flask import Flask, jsonify
from pymongo import MongoClient
import pyspark
from pyspark.sql import SparkSession

class Server:
    def __init__(self, mongo_uri, database, collection_name, spark_app_name):
        self.app = Flask(__name__)
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[database]
        self.collection = self.db[collection_name]
        self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
        self.register_routes()

    def register_routes(self):
        self.app.add_url_rule('/records', view_func=self.get_records, methods=['GET'])

    def get_records(self):
        start_time = time.time()

        # Fetch top 1 million records from MongoDB
        records = list(self.collection.find({}, {'_id': False}).limit(1000000))
        elapsed_time = time.time() - start_time

        if elapsed_time > 180:  # 3 minutes
            # Parallel processing with PySpark
            rdd = self.spark.sparkContext.parallelize(records, numSlices=100)
            top_1m_records = rdd.take(1000000)

            return jsonify(top_1m_records)
        else:
            return jsonify(records)
        print("retrieved records in", elapsed_time, "seconds")

    def run(self, debug=False):
        self.app.run(debug=debug)

if __name__ == '__main__':
    server = Server(
        mongo_uri='mongodb://localhost:27017/',
        database='test1',
        collection_name='tweets',
        spark_app_name='RecordServer'
    )
    server.run(debug=True)
    server.get_records()
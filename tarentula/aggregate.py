import csv
import json
import operator
import sys

from contextlib import contextmanager
from requests.exceptions import HTTPError
from urllib3.exceptions import ProtocolError
from time import sleep

from tarentula.datashare_client import DatashareClient
from tarentula.logger import logger

DEFAULT_AGGREGATION = {
                "contentType": {
                    "terms": {
                        "field": "contentType",
                        "size": 44,
                        "order": {
                            "_count": "desc"
                        }
                    },
                    "aggs": {
                        "bucket_truncate": {
                            "bucket_sort": {
                                "size": 25,
                                "from": 0
                            }
                        }
                    }
                }
            }

class Aggregate:
    def __init__(self,
                 datashare_url: str = 'http://localhost:8080',
                 datashare_project: str = 'local-datashare',
                 output_file: str = 'tarentula_documents.csv',
                 query: str = '*',
                 aggs: str = None,
                #  throttle: int = 0,
                 cookies: str = '',
                 apikey: str = None,
                 elasticsearch_url: str = None,
                 scroll: str = None,
                 traceback: bool = False,
                 type: str = 'Document'):
        self.datashare_url = datashare_url
        self.datashare_project = datashare_project
        self.query = query
        self.aggs = json.loads(aggs) if aggs else DEFAULT_AGGREGATION
        self.output_file = output_file
        # self.throttle = throttle
        self.cookies_string = cookies
        self.apikey = apikey
        self.traceback = traceback
        self.scroll = scroll
        self.type = type
        try:
            self.datashare_client = DatashareClient(datashare_url,
                                                    elasticsearch_url,
                                                    datashare_project,
                                                    cookies,
                                                    apikey)
        except (ConnectionRefusedError, ConnectionError):
            logger.critical('Unable to connect to Datashare', exc_info=self.traceback)
            exit()

    @property
    def query_body(self):
        if self.query.startswith('@'):
            # This file should include the 'aggs' section too
            return self.query_body_from_file
        else:
            return self.query_body_from_string

    @property
    def query_body_from_string(self):
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type": self.type
                            }
                        },
                        {
                            "query_string": {
                                "query": self.query
                            }
                        }
                    ]
                }
            },
            "aggs": self.aggs
        }

    @property
    def query_body_from_file(self):
        with open(self.query[1:]) as json_file:
            query_body = json.load(json_file)
        return query_body

    # def sleep(self):
    #     sleep(self.throttle / 1000)

    def scan_or_query_all(self):
        index = self.datashare_project
        return self.datashare_client.aggregate(index=index, query=self.query_body)

    @contextmanager
    def create_csv_file(self, bucket_field_names):
        with open(self.output_file, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=bucket_field_names)
            writer.writeheader()
            yield writer

    def start(self):
        try:
            buckets = self.scan_or_query_all()
            bucket_field_names = [*buckets[0].keys()]

            with self.create_csv_file(bucket_field_names) as csvwriter:
                for bucket in buckets:
                    csvwriter.writerow(bucket)
                logger.info('Written aggregations in %s' % self.output_file)
        except ProtocolError:
            logger.error('Exception while aggregating documents', exc_info=self.traceback)

import pymongo
from pymongo import errors
import urllib.parse
import logging


class MongoWriter(object):
    """
    MongoDB writer
    """
    db = None
    _skip_id = set()

    def __init__(self, username: str, password: str, host: str, port: int=27017, **kwargs):
        """
        Connect to MongoDB server
        :param username:
        :param password:
        :param host:
        :param port:
        :param kwargs:
        """
        username = urllib.parse.quote_plus(username)
        password = urllib.parse.quote_plus(password)
        logging.info('Connect to MongoDB...')
        self.client = pymongo.MongoClient('mongodb://{}:{}@{}:{}'.format(username, password, host, port))
        if 'database' in kwargs:
            self.db = self.client[kwargs['database']]
        elif 'db' in kwargs:
            self.db = self.client[kwargs['db']]

    def set_db(self, database: str):
        """
        Set MongoDB database
        :param database:
        :return: None
        """
        self.db = self.client[database]

    def get_since_id(self, collection: dict):
        if self.db is None:
            raise ValueError('Database is not available. Setup database first.')
        logging.info('Search last id from {}'.format(collection))
        since_id = self.db[collection].find_one(filter={}, projection={'_id': 1}, sort=[('_id', -1)])
        if since_id is None:
            logging.info('Last id was not found in {}'.format(collection))
            return 0
        else:
            return since_id.get('_id', 0)

    def get_id_list(self, collection: dict):
        if self.db is None:
            raise ValueError('Database is not available. Setup database first.')
        logging.info('Search all id from {}'.format(collection))
        id_list = [x['_id'] for x in self.db[collection].find(filter={}, projection={'_id': 1}, sort=[('_id', -1)])]
        return id_list

    def write(self, data: dict, collection: dict, skip_duplicate: bool=True)
        """
        Write data to MongoDB collection
        :param data:
        :param collection:
        :return: inserted_id
        """
        if self.db is None:
            raise ValueError('Database is not available. Setup database first.')
        if skip_duplicate and data.get('_id') and data.get('_id') in self._skip_id():
            logging.warning('Document _id: {} found in skip id list, skipped'.format(data.get('_id')))
            return -1
        else:
            try:
                result = self.db[collection].insert_one(document=data)
            except errors.DuplicateKeyError as e:
                logging.error(e)
                return -1
            else:
                self._skip_id.add(result.inserted_id)
                return result.inserted_id

    def update(self, data: dict, collection: dict):
        pass

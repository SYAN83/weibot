import logging
from utils import MongoWriter


COLLECTION_MAPPING = {
    'Status': 'statuses',
    'Comment': 'comments',
    'User': 'users'
}


class WeiboObject(object):

    _keys = tuple()

    def __init__(self, data: dict):
        self._data = dict()
        self._data['_id'] = data.get('id', 0)
        print(self._data['_id'])
        if self._keys:
            for key in self._keys:
                if key in data:
                    self._data[key] = data[key]
        else:
            self._data = data.copy()
        self._objects = list()

    def get(self, key, *args, **kwargs):
        return self._data.get(key, *args, **kwargs)

    def get_data(self):
        return self._data

    def write(self, writer: MongoWriter, write_obj: bool=True, recursive: bool=False):
        collection = COLLECTION_MAPPING[self.__class__.__name__]
        logging.info('Write data _id: {} into collection: {}'.format(self._data['_id'], collection))
        result = writer.write(self._data, collection=collection)
        if result == -1:
            logging.warning('Duplicate id found when inserting _id: {} into collection: {}.'.format(self._data['_id'],
                                                                                                    collection))
        else:
            logging.info('Data (_id:{}) inserted into collection: {}'.format(result, collection))
        if write_obj:
            for obj in self._objects:
                obj.write(writer=writer, write_obj=recursive, recursive=recursive)


class Status(WeiboObject):

    def __init__(self, data: dict):
        super().__init__(data=data)
        if 'user' in data:
            self._objects.append(User(data['user']))
            self._data['uid'] = self._objects[-1].get('_id', 0)
        if 'retweeted_status' in data:
            self._objects.append(Status(data=data['retweeted_status']))
            self._data['retweeted_id'] = self._objects[-1].get('_id')
        for key in ['id', 'idstr', 'mid', 'user', 'retweeted_status']:
            self._data.pop(key, None)


class User(WeiboObject):

    def __init__(self, data: dict):
        super().__init__(data=data)
        for key in ['id', 'idstr', 'status']:
            self._data.pop(key, None)


class Comment(WeiboObject):

    def __init__(self, data: dict):
        super().__init__(data=data)
        if 'user' in data:
            self._objects.append(User(data['user']))
            self._data['uid'] = self._objects[-1].get('_id', 0)
        if 'reply_comment' in data:
            self._objects.append(Comment(data=data['reply_comment']))
            self._data['reply_id'] = self._objects[-1].get('_id')
        for key in ['id', 'idstr', 'mid', 'status', 'user', 'reply_comment']:
            self._data.pop(key, None)

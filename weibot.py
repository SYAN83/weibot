import yaml
import weibopy as wb
from weibo_obj import *
from utils import MongoWriter
import logging
from typing import Callable
from functools import partial


FORMAT = '%(asctime)s %(funcName)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)


class Weibot(object):

    def __init__(self, mongo_credentials, weibo_credentials):
        # connect to mongodb
        self.writer = MongoWriter(**mongo_credentials)
        # connect to weibo API
        oauth = wb.OAuthHandler(**weibo_credentials).authorize()
        self.api = wb.API(oauth=oauth)

    def _crawlbot(self, api_func: Callable, obj_class: Callable, obj_name: str, since_last: bool=True):
        max_id = 0
        flag = True
        since_id = 0
        if since_last:
            since_id = self.writer.get_since_id(obj_name)
        while flag:
            try:
                data, _ = api_func(since_id=since_id, max_id=max_id)
            except wb.APIError as e:
                logging.error(e)
                exit(1)
            else:
                records = [r for r in [obj_class(x) for x in data[obj_name]]]
                if max_id > 0:
                    records = [r for r in records if r.get('_id', 0) < max_id]
                logging.info('Total number of records for insertion: {}'.format(len(records)))
                if records:
                    flag = False
                    for record in records:
                        resp = record.write(writer=self.writer, recursive=True)
                        flag = flag or resp
                    max_id = records[-1].get('_id', 0)
                else:
                    break

    def crawl_user_timeline(self, since_last: bool=True):
        user_timeline = partial(self.api.statuses.user_timeline)
        self._crawlbot(api_func=user_timeline, obj_class=Status, obj_name='statuses', since_last=since_last)

    def crawl_comments(self, sid: int, since_last: bool=True):
        comments = partial(self.api.comments.show, id=sid)
        self._crawlbot(api_func=comments, obj_class=Comment, obj_name='comments', since_last=since_last)

    def crawl(self):
        self.crawl_user_timeline()
        for sid in self.writer.get_id_list('statuses'):
            logging.info('Update comments for status _id: {}'.format(sid))
            self.crawl_comments(sid=sid)


if __name__ == '__main__':
    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    weibot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    weibot.crawl()

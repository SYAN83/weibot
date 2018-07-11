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

    skip_id = set()

    def __init__(self, mongo_credentials, weibo_credentials):
        # connect to mongodb
        self.writer = MongoWriter(**mongo_credentials)
        # connect to weibo API
        oauth = wb.OAuthHandler(**weibo_credentials).authorize()
        self.api = wb.API(oauth=oauth)

    def crawler(self, api_func: Callable, obj_class: Callable, obj_name: str):
        max_id = 0
        flag = True
        while flag:
            try:
                data, _ = api_func(max_id=max_id)
            except wb.APIError as e:
                logging.error(e)
                exit(1)
            else:
                flag = False
                records = [r for r in [obj_class(x) for x in data[obj_name]]]
                if max_id > 0:
                    records = [r for r in records if r.get('_id', 0) < max_id]
                logging.info('Total number of records for insertion: {}'.format(len(records)))
                for record in records:
                    resp = record.write(writer=self.writer, recursive=True)
                    flag = flag or resp
                else:
                    max_id = record.get('_id', 0)

    def crawl_user_timeline(self, since_id=0):
        user_timeline = partial(self.api.statuses.user_timeline, since_id=since_id)
        self.crawler(api_func=user_timeline, obj_class=Status, obj_name='statuses')

    def crawl_comments(self, sid: int, since_id: int=0):
        comments = partial(self.api.comments.show, id=sid, since_id=since_id)
        self.crawler(api_func=comments, obj_class=Comment, obj_name='comments')

    def pipeline(self):
        self.writer.get_since_id('statuses')


if __name__ == '__main__':
    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    weibot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    weibot.crawl_comments()

import weibopy as wb
from weibo_obj import *
from utils import MongoWriter
import logging
from typing import Callable, Dict
from functools import partial
import os
import yaml
import time
from requests_oauthlib.oauth2_session import OAuth2Session


FORMAT = '%(asctime)s %(funcName)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

OAUTH = '.oauth.yml'


class Weibot(object):

    def __init__(self, mongo_credentials, weibo_credentials):
        # connect to mongodb
        self.writer = MongoWriter(**mongo_credentials)
        # connect to weibo API
        oauth = self._get_oauth(**weibo_credentials)
        self.api = wb.API(oauth=oauth)

    def _get_oauth(self, **weibo_credentials):
        if os.path.exists(OAUTH):
            with open(OAUTH, 'r') as f:
                oauth_token = yaml.load(f)
            print(oauth_token)
            oauth = OAuth2Session(token=oauth_token)
        else:
            oauth = wb.OAuthHandler(**weibo_credentials).authorize()
            with open(OAUTH, 'w') as f:
                yaml.dump(oauth.token, f)
        return oauth

    def _crawlbot(self, api_func: Callable, obj_class: Callable, obj_name: str,
                  since_last: bool=True, filters: Dict=dict(), suffix: str=''):
        max_id = 0
        flag = True
        since_id = 0
        if since_last:
            since_id = self.writer.get_since_id(collection=obj_name+suffix, filters=filters)
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
                        resp = record.write(writer=self.writer, recursive=True, suffix=suffix)
                        flag = flag or resp
                    max_id = records[-1].get('_id', 0)
                else:
                    break

    def crawl_user_timeline(self, since_last: bool=True, suffix: str=''):
        user_timeline = partial(self.api.statuses.user_timeline)
        self._crawlbot(api_func=user_timeline,
                       obj_class=Status,
                       obj_name='statuses',
                       since_last=since_last,
                       suffix=suffix)

    def crawl_comments(self, sid: int, since_last: bool=True, suffix: str=''):
        comments = partial(self.api.comments.show, id=sid)
        self._crawlbot(api_func=comments,
                       obj_class=Comment,
                       obj_name='comments',
                       since_last=since_last,
                       filters={'sid': sid},
                       suffix=suffix)

    def crawl_friendships(self, suffix: str=''):
        friends, _ = self.api.friendships.friends_ids()
        followers, _ = self.api.friendships.followers_ids()
        friendships = Friendships(friends=friends, followers=followers)
        friendships.write(self.writer, suffix=suffix)

    def crawl(self, suffix: str=''):
        self.crawl_user_timeline(suffix=suffix)
        for sid in self.writer.get_id_list('statuses' + suffix):
            logging.info('Update comments for status _id: {}'.format(sid))
            self.crawl_comments(sid=sid, suffix=suffix)


if __name__ == '__main__':
    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    weibot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    # weibot.crawl_user_timeline(suffix='-test')
    # weibot.crawl()
    weibot.crawl_friendships(suffix='-test')


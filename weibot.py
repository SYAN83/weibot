import yaml
import weibopy as wb
from weibo_data import *
from utils import MongoWriter
import logging

FORMAT = '%(asctime) - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)


class Weibot(object):

    def __init__(self, mongo_credentials, weibo_credentials):
        # connect to mongodb
        self.writer = MongoWriter(**cred['mongo'])
        # connect to weibo API
        oauth = wb.OAuthHandler(**cred['weibo']).authorize()
        self.api = wb.API(oauth=oauth)

    def download_user_timeline(self, since_last=True):
        since_id = 0
        if since_last:
            since_id = self.writer.get_since_id(collection='statuses')
        data, _ = self.api.statuses.user_timeline(since_id=since_id)
        for s_data in data['statuses']:
            status = Status(s_data)
            print(status._data['created_at'], status._data['_id'])
            status.write(writer=self.writer)


if __name__ == '__main__':
    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    main(cred=cred)
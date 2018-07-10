import yaml
import weibopy as wb
from weibo_data import *
from utils import MongoWriter
import logging

FORMAT = '%(asctime) - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)


class Weibot(object):

    skip_id = set()

    def __init__(self, mongo_credentials, weibo_credentials):
        # connect to mongodb
        self.writer = MongoWriter(**mongo_credentials)
        # connect to weibo API
        oauth = wb.OAuthHandler(**weibo_credentials).authorize()
        self.api = wb.API(oauth=oauth)

    # def download_user_timeline(self, since_last=True):
    #     since_id = 0
    #     if since_last:
    #         since_id = self.writer.get_since_id(collection='statuses')
    #     data, _ = self.api.statuses.user_timeline(since_id=since_id)
    #     for s_data in data['statuses']:
    #         status = Status(s_data)
    #         print(status._data['created_at'], status._data['_id'])
    #         status.write(writer=self.writer)

    def crawl_statuses(self, since_id=0, insert: bool=False):
        max_id = 0
        while True:
            data, _ = self.api.statuses.user_timeline(since_id=since_id, max_id=max_id)
            if 'error' in data:
                logging.error(data.get('error', ''))
                return
            records = [s for s in [Status(x) for x in data['statuses']] if s.get('_id') not in self.skip_id]
            logging.info('Total new records: {}'.format(len(records)))
            if records:
                for record in records:
                    if record.get('_id') not in self.skip_id:
                        if insert:
                            record.write(writer=self.writer)
                            self.skip_id.add(record.get('_id'))
                        else:
                            print(record.get('_id'), record.get('created_at'))
                else:
                    max_id = record.get('_id')
            else:
                break


if __name__ == '__main__':
    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    weibot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    weibot.crawl_statuses()

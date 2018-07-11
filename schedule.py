import yaml
import time
from apscheduler.schedulers.background import BackgroundScheduler
from weibot import Weibot

import logging

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def main(duration: int=12, interval: int=1, unit: str='hours'):

    duration = duration
    if unit == 'hours':
        duration *= 3600
    elif unit == 'minutes':
        duration *= 60
    else:
        raise ValueError('Invalid time unit')

    with open('./credentials.yml') as f:
        cred = yaml.load(f)
    weibot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(weibot.crawl, 'interval', **{unit:interval})
    logging.info(job)
    scheduler.start()
    time.sleep(duration)
    scheduler.shutdown(wait=True)


if __name__ == '__main__':
    main()
import yaml
import time
from apscheduler.schedulers.background import BackgroundScheduler
from weibot import Weibot

import logging

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def main(duration: int=24, interval: int=1, time_unit: str='hours'):
    sleep_time = duration
    if duration > 0:
        if time_unit == 'hours':
            sleep_time *= 3600
        elif time_unit == 'minutes':
            sleep_time *= 60
        else:
            raise ValueError('Invalid time unit')
    with open('./credentials.yml', 'r') as f:
        cred = yaml.load(f)
    # create API crawl bot
    bot = Weibot(mongo_credentials=cred['mongo'], weibo_credentials=cred['weibo'])
    # start scheduler
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(bot.crawl, 'interval', **{time_unit: interval})
    logging.info(job)
    job2 = scheduler.add_job(bot.crawl_friendships, 'cron', hour='*')
    logging.info(job2)
    scheduler.start()
    # shut down scheduler
    if duration > 0:
        time.sleep(sleep_time)
        scheduler.shutdown(wait=True)


if __name__ == '__main__':
    main(duration=3, interval=1, time_unit='hours')

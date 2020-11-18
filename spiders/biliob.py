import socket
import asyncio
from time import sleep
from db import async_db, db
import datetime
from simpyder.spiders import AsynSpider
from simpyder import FAKE_UA
from utils import enc
from utils import dec
import os
env_dist = os.environ


class BiliobSpider(AsynSpider):

  async def gen_proxy(self):
    url = env_dist['PYOXIES_URL']
    while True:
      try:
        async with self.sem_gen_proxy:
          res = await self.get(f"{url}/proxies", proxy=None)
        proxies = await res.json()
        self.logger.info(
            f"Get Proxies From: {url}/proxies")
        for proxy in proxies['proxies']:
          self.logger.info(
              f"Get Proxies : {proxy}")
          yield proxy
        await asyncio.sleep(1)
      except Exception as e:
        self.logger.exception(e)

  def __init__(self, name="BiliOB Spider", interval=1.5, concurrency=4, use_proxy=True):
    self.sem_gen_proxy = asyncio.Semaphore(1)
    super().__init__(name=name, user_agent=FAKE_UA,
                     interval=interval, concurrency=concurrency)
    loop = asyncio.get_event_loop()
    self.db = db
    self.async_db = async_db
    self.hostname = socket.gethostname()
    self.use_proxy = use_proxy

  async def mid_gener(self):
    last_data = set()
    while True:
      try:
        # 如果存在锁
        # if self.db.lock.count_documents({"name": "author_data_spider"}):
        #   await asyncio.sleep(0.1)
        #   continue

        # 挂锁
        # await self.async_db['lock'].insert_one(
        #     {"name": "author_data_spider", "date": datetime.datetime.utcnow()})
        data = []

        mc = self.async_db.author_interval.find(
            {'next': {'$lt': datetime.datetime.utcnow()}}).sort([('next', 1)]).limit(100)
        async for d in mc:
          # 手动操作设置为已经执行
          data.append(d)
          if 'order' in d:
            await self.async_db.user_record.update_many({'_id': {'$in': d['order']}}, {'$set': {
                'isExecuted': True
            }})

        # 解锁
        # await self.async_db.lock.delete_one(
        #     {"name": "author_data_spider"})
        tmp_set = set()
        for each_data in data:
          if 'mid' not in each_data:
            self.async_db.author_interval.delete_one({'mid': None})
            self.logger.warning('删除没有mid的author interval记录')
            continue
          if each_data['mid'] not in last_data:
            yield each_data['mid']
            tmp_set.add(each_data['mid'])
        last_data = tmp_set
      except Exception as e:
        self.logger.exception(e)

  async def video_gen_without_lock(self):
    last_data = set()
    batch = 2000
    while True:
      try:
        d = []
        data = self.async_db.video_interval.find(
            {'next': {'$lte': datetime.datetime.utcnow()}}).sort([('next', 1)]).hint("idx_next").limit(batch)
        async for each in data:
          if 'aid' not in each and 'bvid' in each and 'bvid' != '':
            each['aid'] = dec('BV' + each['bvid'].lstrip('BV'))
          elif 'bvid' not in each and 'aid' in each or 'bvid' == '':
            each['bvid'] = enc(each['aid']).lstrip('BV')
          elif 'aid' in each and 'bvid' in each and each['aid'] != None and type(each['aid']) != str and each['aid'] > 0:
            pass
          else:
            await self.async_db.video_interval.delete_one({'_id': each['_id']})
          d.append(each)
        for data in d:
          if 'aid' not in data:
            continue
          if data['aid'] not in last_data:
            last_data.add(data['aid'])
            yield data
        last_data = set()

        if len(d) < batch / 2:
          await asyncio.sleep(10)
      except Exception as e:
        self.logger.exception(e)

  async def update_author_interval_by_mid(self, mid):
    interval_data = await self.async_db.author_interval.find_one({'mid': mid})
    self.logger.debug(f"更新 {mid}")
    await self.update_author_interval(interval_data)

  async def update_author_interval(self, interval_data):
    try:
      if 'interval' in interval_data:
        interval = interval_data['interval']
      else:
        interval = 86400
        interval_data['interval'] = interval
      interval_data['next'] = datetime.datetime.utcnow() + \
          datetime.timedelta(seconds=interval)
      interval_data['order'] = []
      await self.async_db.author_interval.update_one(
          {'mid': interval_data['mid']}, {'$set': interval_data})
    except Exception as e:
      self.logger.exception(e)

  async def total_video_gen(self):
    aid = 55157643
    while True:
      self.logger.info(f'now: {aid}')
      c = self.async_db.video.find({'aid': {"$gt": aid}}, {
                                   'aid': 1}).sort([('aid', 1)]).limit(100)
      async for doc in c:
        aid = doc['aid']
        yield aid

  async def video_gen(self):
    while True:
      # 如果存在锁
      if await self.async_db.lock.count_documents({"name": "video_interval"}):
        sleep(0.1)
        continue
      # 挂锁
      await self.async_db.lock.insert_one(
          {"name": "video_interval", "date": datetime.datetime.utcnow()})
      try:
        d = []
        data = await self.async_db.video_interval.find(
            {'order': {'$exists': True, '$ne': []}}).hint("idx_order").limit(100)
        for each in data:
          d.append(each)
        data = await self.async_db.video_interval.find(
            {'next': {'$lt': datetime.datetime.utcnow()}}).limit(100)
        for each in data:
          d.append(each)
        for data in d:
          # 如果存在手动操作，则刷新数据
          if 'order' in data:
            for order_id in data['order']:
              await self.async_db.user_record.update_one({'_id': order_id}, {'$set': {
                  'isExecuted': True
              }})
          data['next'] = data['next'] + \
              datetime.timedelta(seconds=data['interval'])
          data['order'] = []
          try:
            if 'aid' not in data:
              data['aid'] = dec('BV' + data['bvid'])
              filt = {'bvid': data['bvid']}
            elif 'bvid' not in data:
              data['bvid'] = enc(data['aid']).lstrip("BV")
              filt = {'aid': data['aid']}
            else:
              filt = {'bvid': data['bvid']}
          except Exception:
            if 'aid' in data:
              await self.async_db.video_interval.delete_many({'aid': data['aid']})
            else:
              await self.async_db.video_interval.delete_many({'bvid': data['bvid']})
            continue
          if await self.async_db.video_interval.count(filt) > 1:
            await self.async_db.video_interval.delete_many(filt)

          await self.async_db.video_interval.update_one(
              filt, {'$set': data})

        # 解锁
        await self.async_db.lock.delete_one(
            {"name": "video_interval"})
        for data in d:
          yield data
      except Exception as e:
        self.logger.exception(e)

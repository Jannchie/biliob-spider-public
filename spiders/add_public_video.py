from utils import enc, dec
from biliob import BiliobSpider
from time import sleep
from datetime import datetime
import asyncio
# url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'
url = 'http://api.bilibili.com/x/space/arc/search?mid={}&ps=20&tid=0&pn=1&keyword=&order=pubdate'


class AddPublicVideoSpider(BiliobSpider):
  def __init__(self):
    super().__init__(name='追加作者最新上传的视频', interval=0.1, concurrency=16)
    self.except_content_type = 'application/json'

  async def gen_url(self):
    # yield 'http://api.bilibili.com/x/space/arc/search?mid=326499679&ps=20&tid=0&pn=1&keyword=&order=pubdate'
    while True:
      try:
        cursor = self.async_db.author.find({'$or': [{'cFans': {'$gt': 100000}}, {'forceFocus': True}]},
                                           {'mid': 1}).sort([('biliob.video_update', 1)]).limit(1000).batch_size(1000)
        async for each_author in cursor:
          mid = each_author['mid']
          yield url.format(mid)
      except Exception as e:
        self.logger.exception(e)
      finally:
        await asyncio.sleep(1)

  async def parse(self, res):
    try:
      if res == None:
        return None
      j = res.json_data
      result = []
      if j == None or 'data' not in j or j['data'] == None or 'list' not in j['data'] or 'vlist' not in j['data']['list']:
        return None
      bvid = None
      aid = None
      if len(j['data']['list']['vlist']) == 0:
        return None
      mid = int(res.url.query['mid'])
      channels = j['data']['list']['tlist']
      channel_list = list(channels.values())
      count = sum(map(lambda d: d['count'], channel_list))
      main_channel = '复合'
      for channel in channel_list:
        channel['rate'] = float(channel['count'] / count)
        if channel['rate'] > 0.7:
          main_channel = channel['name']
      await self.async_db.author.update_one(
          {'mid': mid}, {'$set': {'channels': channels, 'main_channel': main_channel, 'channel_list': channel_list}})
      for each_video in j['data']['list']['vlist']:
        if 'bvid' in each_video:
          bvid = each_video['bvid']
        if 'aid' in each_video:
          aid = each_video['aid']
        result.append([bvid, aid, mid])
      return result
    except Exception as e:
      pass
      self.logger.exception(e)

  def update_video_interval(self, interval: int, aid, bvid):
    if aid == None:
      aid = enc(bvid)
    if bvid == None:
      bvid = dec(aid)
    return {
        'next': datetime.utcfromtimestamp(0),
        'interval': interval,
        'aid': aid,
        'bvid': bvid
    }

  async def save(self, items):
    if items == None:
      return 0
    count = 0
    mid = None
    for (bvid, aid, mid) in items:
      bvid = bvid.lstrip('BV')
      interval_data = await self.async_db.video_interval.find_one(
          {'bvid': bvid, 'aid': aid})
      if interval_data == None:
        if await self.async_db.video_interval.find_one({'aid': aid}) != None:
          continue
        await self.async_db.video_interval.update_one(
            {'bvid': bvid, 'aid': aid}, {'$set': self.update_video_interval(3600 * 24, aid, bvid), '$setOnInsert': {'date': datetime.utcnow()}}, upsert=True)
        count += 1
    if mid is not None:
      await self.async_db.author.update_one({'mid': mid}, {'$set': {'biliob.video_update': datetime.utcnow()}}, upsert=True)
    return count


s = AddPublicVideoSpider()


if __name__ == "__main__":

  s.run()

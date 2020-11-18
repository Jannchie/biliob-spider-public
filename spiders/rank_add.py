
import requests
import asyncio
from db import db
import datetime
from biliob import BiliobSpider


def update_interval(interval: int, key: str, value):
  now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
  return {
      'next': now,
      'date': now,
      'interval': interval,
      key: value,
  }


def update_video_interval(interval: int, bvid, aid):
  now = datetime.datetime.utcnow()
  return {
      'next': now,
      'date': now,
      'interval': interval,
      'bvid': bvid,
      'aid': aid
  }


class BiliobRankAdd(BiliobSpider):
  def __init__(self):
    super().__init__("从排行榜获得数据", interval=0, concurrency=8, use_proxy=False)
    self.except_content_type = 'application/json'

  async def gen_url(self):
    while True:
      try:
        online_url = 'http://api.bilibili.com/x/web-interface/online'
        url = 'http://api.bilibili.com/x/web-interface/ranking?rid={}&day=1&type=1&arc_type=0'
        self.logger.info("Get From Online")
        online_data = requests.get(online_url).json()
        rids = online_data['data']['region_count'].keys()
        # data = await self.get(url, self.get_proxy())
        # data = await data.json()
        for rid in rids:
          self.logger.info(f"Crawl rid: {rid}")
          yield url.format(rid)
        await asyncio.sleep(86400)
      except Exception as e:
        self.logger.exception(e)

  async def parse(self, res):
    if res == None:
      return None
    j = res.json_data
    data = j['data']
    items = []
    l = data['list']
    for video_info in l:
      if 'aid' in video_info:
        aid = video_info['aid']
      else:
        aid = None
      bvid = video_info['bvid']
      mid = video_info['mid']
      item = {
          'bvid': bvid.lstrip("BV"),
          'aid': int(aid),
          'mid': int(mid)
      }
      items.append(item)
    return items

  async def save(self, items):
    if items == None:
      return 0
    for item in items:
      author = db.author_interval.find_one({'mid': item['mid']})
      if author == None or author['interval'] > 3600:
        db.author_interval.update_one({
            'mid': item['mid']},
            {
            '$set': update_interval(3600 * 12, 'mid', item['mid'])
        }, upsert=True)
      if 'bvid' in item and item['bvid'] != None:
        filter_dict = {
            'bvid': item['bvid']}
      else:
        filter_dict = {'aid': item['aid']}

      self.db.video_interval.update_one(filter_dict,
                                        {
                                            '$set': update_video_interval(3600 * 12, item['bvid'], item['aid'])
                                        }, upsert=True)
    return len(items)


s = BiliobRankAdd()
if __name__ == "__main__":
  s.run()

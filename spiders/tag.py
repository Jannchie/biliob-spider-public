
from biliob import BiliobSpider
import logging
from utils import enc, dec
import asyncio

from biliob import BiliobSpider
# TODO: https://api.bilibili.com/x/tag/archive/tags?aid=7
# TODO: https://api.bilibili.com/x/tag/archive/tags?bvid=1at411K7R1


class BiliobTagSpider(BiliobSpider):
  def __init__(self):
    super().__init__(name="TAG",
                     interval=0, concurrency=4)

  async def gen_url(self):
    while True:
      try:
        last_tag = await self.async_db.video_info.find_one(
            {'tag': {'$exists': True}},
            {'aid': 1, 'bvid': 1}, sort=[('_id', -1)])
        videos = self.async_db.video_info.find({'_id': {'$gt': last_tag['_id']}}, {
            'aid': 1, 'bvid': 1}).limit(30)
        flag = 0
        async for each_video in videos:
          flag = 1
          if 'bvid' in each_video:
            bvid = each_video['bvid']
          else:
            bvid = enc(each_video['aid'])
          yield 'https://api.bilibili.com/x/tag/archive/tags?bvid={}'.format(bvid)
        if flag == 0:
          await asyncio.sleep(1)
      except Exception as e:
        logging.exception(e)

  async def parse(self, res):
    try:
      json_data = res.json_data

      item = {}
      bvid = res.url.query['bvid']

      item['bvid'] = bvid
      item['aid'] = dec('BV' + bvid)
      if res == None or res.json_data['data'] == None:
        return None
      m = map(lambda x: x['tag_name'], res.json_data['data'])
      if m == None:
        return None
      item['tag_list'] = list(m)
      return item
    except Exception as e:
      self.logger.exception(e)
      return None

  async def save(self, item):
    if item == None:
      return 0
    if item['tag_list'] == []:
      item['tag_list'] = [None]
    if await self.async_db.video_info.find_one({'bvid': item['bvid']}, {'bvid': item['bvid']}) != None:
      try:
        await self.async_db.video_info.update_one({
            'bvid': item['bvid']
        }, {
            '$set': {
                'aid': item['aid'],
                'tag': item['tag_list']
            }
        }, upsert=True)
      except Exception as e:
        pass
    else:
      await self.async_db.video_info.update_one({
          'aid': item['aid']
      }, {
          '$set': {
              'bvid': item['bvid'],
              'tag': item['tag_list']
          }
      }, upsert=True)
    return len(item['tag_list'])


s = BiliobTagSpider()

if __name__ == "__main__":
  s.run()

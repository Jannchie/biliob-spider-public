import datetime
from biliob import BiliobSpider
from cookies_pool import cookies_pool
from fake_useragent import UserAgent
ua = UserAgent()


class CookiesPool():
  def __init__(self):
    self.cookies_pool = cookies_pool
    self.cookies_pool_index = 0
    self.__c = self.__cookies_gener()

  def get_cookies(self):
    return next(self.__c)

  def __cookies_gener(self):
    l = len(self.cookies_pool)
    while True:
      yield cookies_pool[self.cookies_pool_index % l]


class BiliOLiveRoomSpider(BiliobSpider):
  def __init__(self):
    super().__init__("Author Live Spider", 0.1, 8)
    self.cookies_pool = CookiesPool()
    self.except_content_type = 'application/json'
    self.use_proxy = True
    self.retry = 3

  async def gen_url(self):
    while True:
      authors = self.async_db.author.find({'forceFocus': True}, {'mid': 1}).sort(
          [('biliob.live_update', 1)]).limit(100).batch_size(100)
      async for author in authors:
        yield author['mid']
    pass

  async def parse(self, mid):
    try:
      self.headers = {
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.8 Safari/537.36',
          'cookie': self.cookies_pool.get_cookies()
      }
      res = await self.get(f'http://api.bilibili.com/x/space/acc/info?mid={mid}')
      if res == None or res.json_data == None:
        return None
      if 'data' not in res.json_data or 'live_room' not in res.json_data['data']:
        return (mid, None)
      data = res.json_data['data']['live_room']
      return (mid, data)
    except Exception as e:
      self.logger.exception(e)
      return None

  async def save(self, item):
    try:
      if item == None:
        return 0
      mid, data = item
      await self.async_db.author.update_one({'mid': mid}, {
          '$set': {'live_room': data, 'biliob.live_update': datetime.datetime.utcnow()}})
      return item
    except Exception as e:
      self.logger.exception(e)


if __name__ == "__main__":
  s = BiliOLiveRoomSpider()
  s.run()

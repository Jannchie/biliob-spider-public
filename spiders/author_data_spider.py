from db import db
import datetime
from biliob import BiliobSpider
import asyncio
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


class BiliOBAuthorDataSpider(BiliobSpider):
  async def reset_interval(self, reason="任务失败", mid=0):
    self.logger.warning("{}: {}".format(reason, mid))
    return None

  def __init__(self):
    super().__init__("Author Data Spider", 0.1, 8)
    self.cookies_pool = CookiesPool()

    self.except_content_type = 'application/json'
    self.use_proxy = True
    self.retry = 3

    self.crawl_like_and_count = True

  async def gen_url(self):
    # url = '{}://api.bilibili.com/x/space/acc/info?mid={}'

    # url = '{}://api.bilibili.com/x/web-interface/card?mid={}'

    mid_gener = self.mid_gener()
    count = 1
    async for each in mid_gener:
      yield each
      await asyncio.sleep(0)
    else:
      await asyncio.sleep(0)

  async def parse(self, mid):
    try:
      self.headers = {
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.8 Safari/537.36',
          'cookie': self.cookies_pool.get_cookies()
      }
      url = 'http://api.bilibili.com/x/web-interface/card?mid={}&jsonp=jsonp'
      try:
        # self.logger.info('1' + self.proxy)
        res = await self.get(url.format(mid))
        if res == None:
          return await self.reset_interval("解析基础信息出错", mid)
        j = res.json_data
        if 'code' in j and j['code'] == -412:
          return await self.reset_interval("基础信息被Ban", mid)
      except Exception as e:
        # self.logger.exception(e)
        return await self.reset_interval("解析基础信息出错", mid)
      # 删除
      if j['code'] == -400 or j['code'] == -404:
        self.db.author_interval.delete_one({'mid': mid})
        self.logger.warning(j)
        return None
      if 'code' in j and j['code'] == -412:
        return await self.reset_interval("基础信息被Ban", mid)
      name = j['data']['card']['name']
      if mid != int(j['data']['card']['mid']):
        return await self.reset_interval("数据疑似被缓存", mid)
      sex = j['data']['card']['sex']
      face = j['data']['card']['face']
      if 'card' in j and 'data' in j['card'] and j['data']['card'] == None:
        saved_data = db['author'].find_one({'mid': mid})
        if saved_data == None or 'data' not in saved_data:
          db['author_interval'].remove({'mid': mid})
        return await self.reset_interval("解析基础信息出错", mid)
      level = j['data']['card']['level_info']['current_level']
      official = j['data']['card']['Official']['title']
      archive = j['data']['archive_count']
      article = j['data']['article_count']
      fans = j['data']['follower']
      attention = j['data']['card']['attention']
      item = {}
      item['mid'] = int(mid)
      item['name'] = name
      item['face'] = face
      item['official'] = official
      item['sex'] = sex
      item['level'] = int(level)
      item['data'] = {
          'fans': int(fans),
          'attention': int(attention),
          'archive': int(archive),
          'article': int(article),
          'datetime': datetime.datetime.utcnow() + datetime.timedelta(hours=8)
      }
      item['c_fans'] = int(fans)
      item['c_attention'] = int(attention)
      item['c_archive'] = int(archive)
      item['c_article'] = int(article)

      if self.crawl_like_and_count:
        try:
          view_data_res = await self.get(
              "{}://api.bilibili.com/x/space/upstat?mid={}".format('http', mid))
          if view_data_res == None:
            return await self.reset_interval("解析UP主播放、点赞出错", mid)
          j = view_data_res.json_data
          if 'code' in j and j['code'] == -412:
            self.cookies_pool.cookies_pool_index += 1
            return await self.reset_interval("解析UP主播放、点赞被BAN", mid)
        except Exception:
          return await self.reset_interval("解析UP主播放、点赞出错", mid)
        archive_view = j['data']['archive']['view']
        article_view = j['data']['article']['view']
        like = j['data']['likes']
        item['data']['archiveView'] = archive_view
        item['data']['articleView'] = article_view
        item['data']['like'] = like
        item['c_like'] = like
        item['c_archive_view'] = int(archive_view)
        item['c_article_view'] = int(article_view)

      now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
      last_data = self.db.author_data.find_one(
          {'mid': item['mid'], 'datetime': {'$lt': now - datetime.timedelta(1)}})
      if last_data == None:
        last_data = self.db.author_data.find_one(
            {'mid': item['mid']})
        if (last_data != None):
          item['c_rate'] = item['data']['fans'] - last_data['fans']
        else:
          item["c_rate"] = 0
      else:
        delta_seconds = now.timestamp() - last_data['datetime'].timestamp()
        delta_fans = item['data']['fans'] - last_data['fans']
        item['c_rate'] = int(delta_fans / delta_seconds * 86400)
      # self.proxy = await self.proxy_gener.__anext__()
      return item
    except Exception as e:
      self.logger.exception(e)
      return await self.reset_interval(mid)

  async def save(self, item):
    try:
      if item == None:
        return 0
      mid = item['mid']
      s = {
          'focus': True,
          'sex': item['sex'],
          'name': item['name'],
          'face': item['face'],
          'level': item['level'],
          'cFans': item['c_fans'],
          'cRate': item['c_rate'],
          'cLike': item['c_like'],
          'cArchive_view': item['c_archive_view'],
          'cArticle_view': item['c_article_view'],
          'cArchive': item['c_archive'],
          'cArticle': item['c_article'],
          'official': item['official'],
          'data': item['data']
      }
      await self.async_db.author.update_one({
          'mid': item['mid']
      }, {
          '$set': s
      }, True)

      item['data']['mid'] = item['mid']
      await self.async_db.author_data.replace_one(
          {'mid': item['data']['mid'],
           #  'src': self.hostname,
           'datetime': item['data']['datetime']}, item['data'], upsert=True)
      await self.update_author_interval_by_mid(mid)
      return item
    except Exception as e:
      self.logger.exception(e)
      await self.reset_interval("存储失败", item['mid'])


if __name__ == "__main__":
  s = BiliOBAuthorDataSpider()
  s.run()

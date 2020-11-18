
from time import sleep
from db import db
from biliob import BiliobSpider
import logging


class BiliobAuthorFollowSpider(BiliobSpider):

  def __init__(self):
    super().__init__("UP主关注爬虫")
    self.except_content_type = 'application/json'
    pass

  async def gen_url(self):
    ps = 50
    mid = 0
    pn_list = [1, 2, 3]
    url = 'http://api.bilibili.com/x/relation/followings?vmid={mid}&pn={pn}&ps={ps}'
    while True:
      try:
        authors = db.author.find(
            {'mid': {'$gt': mid}},
            {'mid': 1}).limit(30)
        flag = False
        for each_author in authors:
          flag = True
          for pn in pn_list:
            yield url.format(mid=each_author['mid'], pn=pn, ps=ps)
        if flag == False:
          mid = 0
      except Exception as e:
        logging.exception(e)
        sleep(10)

  async def parse(self, res):
    try:
      j = res.json_data
      item = {
          'mid': int(str(res.url).split('?')[1].split('&')[0].split('=')[1]),
          'follows': []
      }
      for each_member in j['data']['list']:
        item['follows'].append(each_member['mid'])
    except Exception as e:
      self.logger.exception(e)
      return None
    return item

  async def save(self, item):
    if item == None:
      return 0
    db.author.update_one({'mid': item['mid']}, {'$addToSet': {
        'follows': {'$each': item['follows']}
    }})
    return 1


if __name__ == "__main__":
  s = BiliobAuthorFollowSpider()
  s.run()

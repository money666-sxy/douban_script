import urllib.request
from bs4 import BeautifulSoup
import re
import requests
import pg_handle
import time
from lxml import etree, html


def sidList():
    '''连接数据库，取出sid列表'''
    sid_list = []
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    sid_seq = pg.query('SELECT sid  FROM public."bookInfo_bookinfo";')
    for sid in sid_seq:
        sid = eval((str(sid).strip('(').strip(')').strip(',')))
        sid_list.append(sid)
    return sid_list


def get_html(target_url):
    '''伪装头部，向目标url发出请求，获取页面内容'''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'bid=7h4c4NfRjH0; douban-fav-remind=1; __yadk_uid=zHPdpsl2bi1zikBlNH1wf1hhCrooZ7Aj; ll="118371"; __gads=ID=0bbad76ae4b2c5e5:T=1581502187:S=ALNI_Ma9BDbSKBYEYHpxmO3i6Zibq03J5Q; _vwo_uuid_v2=D8AE9E77872632933C6340367A687A470|230a5124e9df6edc169e8b4f1fce563a; viewed="20427187"; gr_user_id=732909fb-06f0-4e9f-9acf-7d61b844c1ff; __utmc=30149280; dbcl2="211811562:O2/VqU4bv+I"; ck=PaIt; push_noty_num=0; push_doumail_num=0; __utmv=30149280.21181; douban-profile-remind=1; __utmc=81379588; __utmz=81379588.1582535506.3.3.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/search; __utmz=30149280.1582709362.17.15.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03=6773c39a-91f8-4956-a08a-93f16f1c2dae; gr_cs1_6773c39a-91f8-4956-a08a-93f16f1c2dae=user_id%3A1; ap_v=0,6.0; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03_6773c39a-91f8-4956-a08a-93f16f1c2dae=true; _pk_ref.100001.3ac3=%5B%22%22%2C%22%22%2C1583162791%2C%22https%3A%2F%2Fwww.douban.com%2Fsearch%3Fsource%3Dsuggest%26q%3D%25E5%258A%25A0%25E5%258B%2592%25E6%25AF%2594%25E6%25B5%25B7%25E7%259B%25972%22%5D; _pk_id.100001.3ac3=7df6f5761810d427.1569876939.4.1583162791.1582535506.; _pk_ses.100001.3ac3=*; __utma=30149280.339508517.1581502187.1582973812.1583162791.19; __utmb=30149280.1.10.1583162791; __utma=81379588.2041522979.1569876940.1582535506.1583162791.4; __utmb=81379588.1.10.1583162791',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
    }
    req = urllib.request.Request(target_url, headers=headers)
    req = urllib.request.urlopen(req)
    book = req.read().decode('utf-8')
    return book


def Sql_insert_sts(list):
    '''将爬取到的星级、评分、标签写入数据库'''
    a = str(list).strip('[').strip(']')
    sql_insert = '''INSERT INTO public."bookInfo_bookinfo" 
        (STAR,TAG,SCORE) 
        VALUES ('{0}');'''.format(str(a))
    print(sql_insert)
    return sql_insert


def creat_sidurl(sid):
    '''输入sid，生成目标url'''
    url = 'https://book.douban.com/subject/'+str(sid)+'/'
    return url


# def star_(tree):
#     '''解析页面，将星级处理成字典'''
#     raw_stars = tree.xpath(
#         '//div[@class="rating_right "]/*')[0]  # find stars
#     raw_stars = html.tostring(raw_stars).decode(encoding='utf-8')
#     stars = {}  # type == dict
#     # value of 'stars' dict
#     stars_values = tree.xpath('//span[@class="rating_per"]')
#     length = len(stars_values)
#     for i in range(length):
#         stars_key = str(length-i) + '星'
#         stars[stars_key] = float(str(stars_values[i].text).strip('%'))
#         # print(stars[stars_key])
#     return stars

# def tags_(tree):
#     '''解析页面，将tag处理成列表'''
#     raw_tags = tree.xpath('//a[@class="  tag"]')  # find tags
#     tags = []  # type == list
#     for raw_tag in raw_tags:
#         tags.append(raw_tag.text)
#     return tags


def star_str(tree):
    '''解析页面，处理星级'''
    raw_stars = tree.xpath(
        '//div[@class="rating_right "]/*')[0]  # find stars
    raw_stars = html.tostring(raw_stars).decode(encoding='utf-8')
    stars_values = tree.xpath('//span[@class="rating_per"]')
    length = len(stars_values)
    stars_num = ''
    for i in range(length):
        # stars_key = str(length-i) + '星'
        stars_ = str(stars_values[i].text).strip('%')  # 去除百分号
        stars_num = stars_num + '/' + stars_  # 将数据处理成 5/4/3/2/1 格数
        # print(stars[stars_key])
    return stars_num


def score_(tree):
    '''解析页面，将score处理成浮点数 '''
    raw_score = tree.xpath(
        '//strong[@class="ll rating_num "]')[0]  # find score
    score = float(raw_score.text)  # type == float
    return score


def tags_str(tree):
    '''解析页面，将标签处理成字符串格式 中国/历史/传记 '''
    raw_tags = tree.xpath('//a[@class="  tag"]')  # find tags
    tags = ''  # type == str
    for raw_tag in raw_tags:
        tags = tags + '/' + raw_tag.text
    return tags


def sst():
    '''从数据库中取出sid列表，爬取内容并写入数据库bookinfo'''
    sid_list = sidList()
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    for sid in sid_list:
        crawal_sst(sid)


def crawal_sst(sid):
    '''爬取页面内容并写入数据库'''
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    url = creat_sidurl(int(sid))  # 生成目标url
    content = get_html(url)  # 爬取页面内容
    tree = etree.HTML(content)  # 转化成etree
    # score = score_(tree)
    # star = star_str(tree)
    # tags = tags_(tree)
    # database must str,not dist or list
    star = star_str(tree)  # 解析星级
    score = score_(tree)  # 解析评分
    tags = tags_str(tree)  # 解析标签
    sql_insert = '''UPDATE public."bookInfo_bookinfo" 
                    set star = '{0}',tag='{1}',score={2} 
                    WHERE sid={3};'''.format(star, tags, score, sid)
    print(sql_insert)
    pg.execute(sql_insert)


if __name__ == "__main__":
    sst()

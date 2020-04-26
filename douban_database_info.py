import urllib.request
from bs4 import BeautifulSoup
import re
import requests
import pg_handle
import time


def sql_table():
    sql_table = '''CREATE TABLE BOOK_INFO2
       (SID           INT PRIMARY KEY ,
       NAME           TEXT,
       STAR           TEXT,
       TAG            TEXT,
       SCORE          TEXT,
       INFO           TEXT,
       TFIDF          TEXT);'''
    return sql_table


def Sql_insert_sid(list_):
    # a = list(list_)
    sql_insert = 'INSERT INTO '+'book_info' + \
        '(sid,name) VALUES ('+str(list_)+');'
    return sql_insert


def get_html(target_url):
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


def get_content_sid(target_url):
    """获取url页面"""
    books = []
    book = get_html(target_url)
    soup = BeautifulSoup(book, 'lxml')  # 使用lxml作为解析器，返回一个Beautifulsoup对象
    # 找到其中所有width=100%的table标签），即找到所有的书
    table = soup.findAll('table', {"width": "100%"})
    for item in table:  # 遍历table，一个item代表一本书
        name = item.div.a.text.strip()  # 找到书名
        # 通过看网页的HTML结构，可以发现书名后是有换行以及空格的，将这些全部通过replace替换去除
        r_name = name.replace('\n', '').replace(' ', '')
        tmp2 = item.div.span  # 判断是否存在别名
        if tmp2:
            # 因为是通过div.span判断别名 有些书的别名前面有个冒号，比如《三体系列》
            name2 = tmp2.text.strip().replace(':', '')
        else:
            name2 = r_name  # 无别名就使用原始的名称
        sid = str(item.div.a['href']).split('/')[-2]  # 获取书链接的sid

        books.append((sid, r_name))  # 以元组存入列表
    return books  # 返回一页的书籍


def book_info():
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    # pg.execute(sql_table())
    try:
        for n in range(2):
            print('正在爬取第{0}页的数据'.format(n+1))
            url1 = 'https://book.douban.com/top250?start=' + \
                str(n*25)  # top250的网页，每页25本书，共10页，“start=”后面从0开始，以25递增
            tmp = get_content_sid(url1)
            print(tmp)
            for a in tmp:
                sql_insert = Sql_insert_sid(str(a).strip('(').strip(')'))
                pg.execute(sql_insert)
            time.sleep(5)
    except IndexError:
        print('第{0}页存储错误'.format(n+1))
        pass


if __name__ == "__main__":
    book_info()

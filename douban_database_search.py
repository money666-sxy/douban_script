import ssl
import string
import urllib
import urllib.request
import urllib.parse
import pg_handle
import time
from bs4 import BeautifulSoup
import douban_database_info
import douban_database_comment
import douban_database_sst
import douban_database_pesg
import douban_database_tf_wc


def create_url(keyword: str, kind: str) -> str:
    '''
    Create url through keywords
    Args:
        keyword: the keyword you want to search
        kind: a string indicating the kind of search result
            type: 读书; num: 1001
            type: 电影; num: 1002
            type: 音乐; num: 1003
    Returns: url
    '''
    num = ''
    if kind == '读书':
        num = 1001
    elif kind == '电影':
        num = 1002
    elif kind == '音乐':
        num = 1003
    url = 'https://www.douban.com/search?cat=' + \
        str(num) + '&q=' + keyword
    return url


def get_html(url: str) -> str:
    '''send a request'''

    headers = {
        # 'Cookie': 你的cookie,
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
        'Connection': 'keep-alive'
    }
    ssl._create_default_https_context = ssl._create_unverified_context

    s = urllib.parse.quote(url, safe=string.printable)  # safe表示可以忽略的部分
    req = urllib.request.Request(url=s, headers=headers)
    req = urllib.request.urlopen(req)
    content = req.read().decode('utf-8')
    return content


def get_content(keyword: str, kind: str) -> str:
    '''
    Create url through keywords
    Args:
        keyword: the keyword you want to search
        kind: a string indicating the kind of search result
            type: 读书; num: 1001
            type: 电影; num: 1002
            type: 音乐; num: 1003
    Returns: url
    '''
    url = create_url(keyword=keyword, kind=kind)
    html = get_html(url)
    # print(html)
    soup_content = BeautifulSoup(html, 'html.parser')
    contents = soup_content.find_all('h3', limit=1)
    result = str(contents[0])
    return result


def find_sid(raw_str: str) -> str:
    '''
    find sid in raw_str
    Args:
        raw_str: a html info string contains sid
    Returns:
        sid
    '''
    assert type(raw_str) == str, \
        '''the type of raw_str must be str'''
    start_index = raw_str.find('sid:')
    end_index = raw_str.find('qcat')
    sid = raw_str[start_index + 5: end_index-2]
    # sid = raw_str[start_index + 5: start_index + 14]
    sid.strip(',')
    return sid


def search_sid(keywords: str):
    kind = '读书'
    raw_str = get_content(keywords, kind)
    sid = find_sid(raw_str)
    # print(sid)
    return sid


def search_main(keywords: str):
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    sid = search_sid(keywords)
    sql_insert = '''INSERT INTO public."bookInfo_bookinfo"
                    (sid,name)
                    VALUES ({0},'{1}');'''.format(sid, keywords)
    pg.execute(sql_insert)  # 像表内插入书名、sid
    douban_database_sst.crawal_sst(sid)  # 爬取图书评分、星级、标签
    douban_database_comment.book_crawler(sid)  # 爬取图书评论

    douban_database_pesg.crawal_pesg(sid)  # 提取评论词性
    douban_database_tf_wc.crawal_tf_wc(sid)  # 提取文本特征并制作词云图


if __name__ == "__main__":
    # sid('看见', '读书')
    # sid = search_sid('红楼梦')
    # print(sid)

    start = time.time()
    search_main('见识')
    print(time.time()-start)

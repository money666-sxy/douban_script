# coding=utf-8
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import time
import pg_handle


def creat_url(num, pages):
    '''生成待爬取url列表，返回为字典'''
    urls = {}
    for pages in range(1, pages+1):
        url = 'https://book.douban.com/subject/' + \
            str(num)+'/comments/hot?p='+str(pages)+''
        urls[pages] = url
    return urls


def get_html(url):
    '''伪装头部，向目标url发出请求，获取页面内容'''
    headers = {
        # 'Cookie': 你的cookie,
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
        'Connection': 'keep-alive'
    }
    print('正在爬取：'+url)
    req = urllib.request.Request(url=url, headers=headers)
    req = urllib.request.urlopen(req)
    content = req.read().decode('utf-8')
    return content


def Sql_insert(sid, list):
    '''输入sid、已爬取内容，生成插入sql语句'''
    a = str(list).strip('[').strip(']')
    sql_insert = '''INSERT INTO public."bookInfo_bookcomment" 
                     (SID, NUM, NAME, TIME, LIKE_NUM, COMMENT) 
                     VALUES({0}, {1})'''.format(str(sid), str(a))
    return sql_insert


def get_content(page, num, html):
    '''
    输入评论页码数、页面评论序号（一页20条）、待解析页面
    使用beautiful soup库解析页面，生成作者、时间、点赞数、评论列表
    返回id、作者、时间、点赞数、评论列表切片 （组成的data列表）
    '''
    soup_content = BeautifulSoup(html, 'html.parser')
    data = []
    data.append(str((page-1)*20+num+1))

    names = soup_content.select('.comment-item > div > h3 > .comment-info > a')
    onePageNames = []
    for name in names:
        onePageNames.append(name.getText())
    data.append(onePageNames[num])

    times = soup_content.select(
        '.comment-item > div > h3 > .comment-info > span:nth-of-type(2)')
    onePageTimes = []
    for time in times:
        onePageTimes.append(time.getText())
    data.append(onePageTimes[num])

    like_nums = soup_content.select(
        '.comment-item > div > h3 > .comment-vote > span')
    onePageLike = []
    for like in like_nums:
        onePageLike.append(like.getText())
    data.append(onePageLike[num])

    comments = soup_content.findAll('span', 'short')
    onePageComments = []
    for comment in comments:
        onePageComments.append(comment.getText())
    data.append(onePageComments[num])
    print(data)
    return data


def book_crawler(sid: int):
    '''输入图书唯一标识sid，爬取、解析、插库'''
    pages = 20
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    urls = creat_url(sid, pages)
    for page, url in urls.items():
        try:
            html = get_html(url)  # 爬取
            for num in range(20):
                list_ = get_content(page, num, html)  # 解析
                sql_insert = Sql_insert(sid, list_)  # 生成sql语句
                pg.execute(sql_insert)  # 插库
                # print(sql_insert) #打印sql语句
            time.sleep(10)  # 爬取一个页面等待十秒，防止豆瓣反爬
        except IndexError:
            print('第{0}页，数量不足或溢出'.format(page))
            pass


def sidList():
    '''从bookinfo库中取出待爬取sid，返回sid列表'''
    sid_list = []
    pg = pg_handle.PgHandler("postgres", "postgres", "6666")
    sid_seq = pg.query('SELECT sid  FROM public."bookInfo_bookcomment" ;')
    for sid in sid_seq:
        sid = eval((str(sid).strip('(').strip(')').strip(',')))
        sid_list.append(sid)
    return sid_list[50:]  # 返回列表长度


def database_comment():
    '''
    生成sid列表
    爬取所有评论相关信息（一本图书对应一个sid，一本图书大约有400条评论（20*20））
    '''
    sid_list = sidList()
    for sid in sid_list:
        book_crawler(sid)


if __name__ == "__main__":
    # book_crawler(1007305)
    database_comment()

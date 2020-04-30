import jieba.posseg as pseg
import time
import psycopg2
import traceback
import jieba
import json
jieba.posseg.POSTokenizer(tokenizer=jieba.Tokenizer())


class PgHandler(object):
    '''
    pgsql
    '''

    def __init__(self, db="postgres", user="postgres", password=None, host="127.0.0.1", port="5432"):
        '''
        Args:
            db: Database
            user: Username
            password: Password
            host: Server
            port: Port
        '''
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.config = f'dbname = {self.db} \
                        user = {self.user} \
                        password = {self.password} \
                        host = {self.host} \
                        port = {self.port}'

    def query(self, sql):
        '''
        Args:
            sql: sql you want to query
        '''
        try:
            conn = psycopg2.connect(self.config)
            cur = conn.cursor()
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            if not res:
                res = []
            return res
        except psycopg2.Error as e:
            print(str(traceback.format_exc()))
            return

    def execute(self, sql):
        '''
        Args:
            sql: sql you want to execute
        '''
        try:
            conn = psycopg2.connect(self.config)
            cur = conn.cursor()
            cur.execute(sql)
            res = None
            if "returning" in sql:
                res = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            return res
        except psycopg2.Error as e:
            print(str(traceback.format_exc()))


def sidList():
    sid_list = []
    pg = PgHandler("postgres", "postgres", "6666")
    sid_seq = pg.query('SELECT sid  FROM public."bookInfo_bookinfo";')
    for sid in sid_seq:
        sid = eval((str(sid).strip('(').strip(')').strip(',')))
        sid_list.append(sid)
    return sid_list


def pos_ch(tf_pos_dict: dict):
    '''
    词性频率列表转化成中文标识
    词性列表来源于结巴-> https://github.com/fxsjy/jieba
    '''
    tf_dict = {}
    for k, v in tf_pos_dict.items():
        if k == 'n':
            tf_dict["普通名词"] = v
        if k == 'f':
            tf_dict["方位名词"] = v
        if k == 's':
            tf_dict["处所名词"] = v
        if k == 't':
            tf_dict["时间"] = v
        if k == 'nr':
            tf_dict["人名"] = v
        if k == 'ns':
            tf_dict["地名"] = v
        if k == 'nt':
            tf_dict["机构名"] = v
        if k == 'nw':
            tf_dict["作品名"] = v
        if k == 'nz':
            tf_dict["其他名词"] = v
        if k == 'v':
            tf_dict["普通动词"] = v
        if k == 'vd':
            tf_dict["动副词"] = v
        if k == 'vn':
            tf_dict["动名词"] = v
        if k == 'a':
            tf_dict["形容词"] = v
        if k == 'ad':
            tf_dict["形副词"] = v
        if k == 'an':
            tf_dict["名形词"] = v
        if k == 'd':
            tf_dict["副词"] = v
        if k == 'm':
            tf_dict["数量词"] = v
        if k == 'q':
            tf_dict["量词"] = v
        if k == 'r':
            tf_dict["代词"] = v
        if k == 'p':
            tf_dict["介词"] = v
        if k == 'c':
            tf_dict["连词"] = v
        if k == 'u':
            tf_dict["助词"] = v
        if k == 'xc':
            tf_dict["其他虚词"] = v
        if k == 'w':
            tf_dict["标点符号"] = v
        if k == 'PER':
            tf_dict["人名"] = v
        if k == 'LOC':
            tf_dict["地名"] = v
        if k == 'ORG':
            tf_dict["机构名"] = v
        if k == 'TIME':
            tf_dict["时间"] = v
    # print(tf_dict) #打印词性字典
    return tf_dict


def pesg_word(sid):
    # 从数据库取出评论处理成词性列表
    pos_dict = {}
    tf_pos_dict = {}
    pos_list = []
    pg = PgHandler("postgres", "postgres", "6666")
    datas = pg.query(
        'SELECT comment FROM public."bookInfo_bookcomment" WHERE (SID = {0});'.format(sid))
    for i in datas:
        pos_dict.update(dict(pseg.lcut(str(i).strip('(').strip(')'))))
    for j in pos_dict.values():
        if j in tf_pos_dict:
            tf_pos_dict[j] += 1
        else:
            tf_pos_dict[j] = 1
    tf_pos_dict = dict(
        sorted(tf_pos_dict.items(), key=lambda item: item[1], reverse=True))
    # print('词性字典：', tf_pos_dict)
    print('一共有{0}个词汇'.format(sum(tf_pos_dict.values())))
    tf_pos_dict = pos_ch(tf_pos_dict)

    return tf_pos_dict
    # return pos_list  # 处理成前端所需格式


def pos_list(tf_pos_dict):
    # 将词频字典转化为前端所需的json格式
    pos_list = []
    # tf_pos_dict = pesg_word(sid)
    for i, j in tf_pos_dict.items():
        pos_list.append({"value": j, "name": i})
    # print(pos_list)
    pos_json = json.dumps(pos_list, ensure_ascii=False)
    # print(pos_json)
    # return pos_list
    return pos_json


def pos_name(tf_pos_dict):
    # 将词频字典转化为前端所需的词性列表格式
    # dict_ = pesg_word(sid)
    # print(list(dict_.keys()))
    pesg_json_name = json.dumps(list(tf_pos_dict.keys()), ensure_ascii=False)
    return pesg_json_name


def main():
    pg = PgHandler("postgres", "postgres", "6666")
    sid_list = sidList()
    for sid in sid_list:
        crawal_pesg(sid)
        # pos_dict = pesg_word(sid)
        # pesg_list_json = pos_list(pos_dict)
        # pesg_name_json = pos_name(pos_dict)
        # sql_insert_list = '''UPDATE public."bookInfo_bookinfo" SET
        #             pesg_list = '{0}'::jsonb
        #             WHERE sid = {1}; '''.format(pesg_list_json, sid)
        # print(sql_insert_list)
        # pg.execute(sql_insert_list)
        # sql_insert_name = '''UPDATE public."bookInfo_bookinfo" SET
        #             pesg_name = '{0}'::jsonb
        #             WHERE sid = {1}; '''.format(pesg_name_json, sid)
        # pg.execute(sql_insert_name)
        # print(sql_insert_name)


def crawal_pesg(sid):
    pg = PgHandler("postgres", "postgres", "6666")
    pos_dict = pesg_word(sid)
    pesg_list_json = pos_list(pos_dict)
    pesg_name_json = pos_name(pos_dict)
    sql_insert_list = '''UPDATE public."bookInfo_bookinfo" SET
                    pesg_list = '{0}'::jsonb
                    WHERE sid = {1}; '''.format(pesg_list_json, sid)
    print(sql_insert_list)
    pg.execute(sql_insert_list)
    sql_insert_name = '''UPDATE public."bookInfo_bookinfo" SET
                    pesg_name = '{0}'::jsonb
                    WHERE sid = {1}; '''.format(pesg_name_json, sid)
    pg.execute(sql_insert_name)
    print(sql_insert_name)


if __name__ == "__main__":
    # sid = 1007305
    start = time.time()
    main()
    # pos_dict = pesg_word(sid)
    # pesg_list_json = pos_list(pos_dict)
    # pos_name(pos_dict)
    # sql_insert = '''UPDATE public."bookInfo_bookinfo" SET
    #                 pesg_list = '{0}'::jsonb
    #                 WHERE sid = 1007305; '''.format(pesg_list_json)

    # pg.execute(sql_insert)
    # 查看词性列表（json格式）
    # sql_query = '''SELECT pesg_list
    #                FROM public."bookInfo_bookinfo"
    #                WHERE sid = 1007305;'''
    # data = pg.query(sql_query)
    # print(data, type(data))
    print(time.time()-start)

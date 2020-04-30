import jieba.posseg as pseg
import time
import psycopg2
import traceback
import jieba
import json
from pg_handle import PgHandler
jieba.posseg.POSTokenizer(tokenizer=jieba.Tokenizer())


def sidList():
    '''取出sid列表'''
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
    pg = PgHandler("postgres", "postgres", "6666")
    datas = pg.query(
        'SELECT comment FROM public."bookInfo_bookcomment" WHERE (SID = {0});'.format(sid))
    for i in datas:  # 遍历词性评论并用jieba.pesg处理成词性字典 （词1:n；词2:v）
        pos_dict.update(dict(pseg.lcut(str(i).strip('(').strip(')'))))
    for j in pos_dict.values():  # 计算相同词性出现的频数，存入字典
        if j in tf_pos_dict:
            tf_pos_dict[j] += 1
        else:
            tf_pos_dict[j] = 1
    tf_pos_dict = dict(
        sorted(tf_pos_dict.items(), key=lambda item: item[1], reverse=True))  # 按照频数为词性字典排序
    # print('词性字典：', tf_pos_dict)
    print('一共有{0}个词汇'.format(sum(tf_pos_dict.values())))
    tf_pos_dict = pos_ch(tf_pos_dict)  # 转化为中文格式的词性
    return tf_pos_dict
    # return pos_list  # 处理成前端所需格式


def pos_list(tf_pos_dict):
    # 将词频字典转化为前端所需的json格式
    pos_list = []
    # tf_pos_dict = pesg_word(sid)
    for i, j in tf_pos_dict.items():
        pos_list.append({"value": j, "name": i})
    # 此方法专门用于字典（单引号）转换为json格式（双引号）
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
    # 取出sid列表，计算出词性名列表、词性频数字典转化为json格式存入数据库
    sid_list = sidList()
    for sid in sid_list:
        crawal_pesg(sid)


def crawal_pesg(sid):
    pg = PgHandler("postgres", "postgres", "6666")
    pos_dict = pesg_word(sid)  # 计算词性频数字典
    pesg_list_json = pos_list(pos_dict)  # 转化成前端所需词性频数字典json格式
    pesg_name_json = pos_name(pos_dict)  # 转化成前端所需词性名称列表json格式
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
    print(time.time()-start)

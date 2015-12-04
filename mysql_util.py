#encoding=utf8
import os, re
import time
from bson.objectid import ObjectId
from mvc import *

def Conn(db_host = None, db_user = None, db_password = None, db_name=None, **kwargs):
    db_host = db_host or __conf__.MYSQL_HOST
    db_user = db_user or __conf__.MYSQL_USER
    db_password = db_password or __conf__.MYSQL_PASSWORD
    db_name = db_name or __conf__.MYSQL_NAME
    return get_context().get_mysql(db_host, db_user, db_password, db_name)

def m_execute(sql, **kwargs):
    """
        保存数据
    """
    conn = Conn(**kwargs)
    conn.cursor().execute(sql)
    conn.commit()

def m_query_one(sql, fields,**kwargs):
    """
        查询保存数据
    """
    conn = Conn(**kwargs)
    cur = conn.cursor()
    cur.execute(sql)
    rs = cur.fetchall()

    if rs:
        rs = rs[0]
        return dict((fields[i], rs[i]) for i in range(len(rs)))
    return {}


def m_query(sql, fields, **kwargs):
    """
        查询保存数据
    """
    page_index = int(kwargs.pop('page_index', 1))
    page_size = int(kwargs.pop('page_size', 10))
    findall = kwargs.pop('findall', None)

    sql_count = re.sub("select.*from", "select count(*) from", sql)
    count = m_query_one(sql_count, ('count', ))['count']

    if count and findall:
        page_index = 1
        page_size = count

    page_num = (count + page_size - 1)/ page_size
    if page_num < page_index:
        page_index = page_num
    page = dict(page_index = page_index, page_size = page_size, page_num = page_num,allcount=count)
    if page_num == 0: return {}, page

    sql += " limit {},{}".format((page_index-1)*page_size,page_size)

    conn = Conn(**kwargs)
    cur = conn.cursor()
    cur.execute(sql)
    rs = cur.fetchall()
    data = []
    if fields:
        for r in rs:
            d = dict((fields[i], r[i]) for i in range(len(r)))
            data.append(d)

    return data, page



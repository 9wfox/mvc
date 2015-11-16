#encoding=utf8
import os
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

def m_query(sql, **kwargs):
    """
        查询保存数据
    """
    conn = Conn(**kwargs)
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


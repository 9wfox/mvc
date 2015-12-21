# -*- coding:utf-8 -*-

"""
    逻辑模块

    LogicContext 封装了逻辑代码执行所需的上下文环境和对象。
    应该使用 with sgement 执行 LogicContext，确保资源被释放。

    历史:
        2011-08-04  * 增加 _redis_send_srv_pack。
        2011-08-07  + 增加 get_context 获取有效上下文对象。
        2011-08-18  + 增加 args_range, args_length。
        2011-08-25  * 将参数检查函数转移到 utility 中。
        2011-08-25  * redis_host 支持 "host:port" 格式。
        2011-08-30  + ConsistentHash 增加缓存，已提高效率。
"""

from sys import maxint
from bisect import bisect_right
from hashlib import md5
from threading import local

from redis import Redis
import pymongo
from gridfs import GridFS



class ConsistentHash(object):
    """
        一致性哈希算法
    """

    _caches = {}

    def __init__(self, hosts, replicas = 10):
        self._hosts = {}
        self._ring = []
        self._build(hosts, replicas)


    def _build(self, hosts, replicas):
        for host in hosts:
            for i in xrange(replicas):
                key = "{0}_{1}".format(host, i)
                hsh = self._hash(key)

                self._hosts[str(hsh)] = host
                self._ring.insert(bisect_right(self._ring, hsh), hsh)            


    def _hash(self, s):
        return hash(md5(s).digest()) % 10000


    def get_host(self, key):
        hsh = self._hash(key)
        index = bisect_right(self._ring, hsh)
        if index >= len(self._ring): index = 0

        return self._hosts[str(self._ring[index])]


    @classmethod
    def get(cls, hosts):
        """
            从缓存中重复使用哈希环
        """
        key = str(hosts)
        if key not in cls._caches: cls._caches[key] = cls(hosts)
        return cls._caches[key]




class LogicContext(object):
    """
        逻辑上下文

        共享服务器连接。
        
        (1) 支持多个 Redis Cache Server，如: ("localhost", "192.168.1.8:9000")。
        (2) 数据库服务器，如: "localhost" 或 "192.168.1.8:27000"。
        (3) 可以在 settings.py 中修改默认设置。
    """

    # 多线程独立存储
    _thread_local = local()

    def __init__(self, cache_hosts = None):
        self._cache_hashs = ConsistentHash.get(cache_hosts or __conf__.CACHE_SERVERS)
        self._caches = {}
        self._cache_cluster = None


    def __enter__(self):
        if not hasattr(self._thread_local, "contexts"): self._thread_local.contexts = []
        self._thread_local.contexts.append(self)
        return self


    def __exit__(self, exc_type, exc_value, trackback):
        self._thread_local.contexts.remove(self)
        self.close()


    def open(self):
        pass


    def close(self):
        for cache in self._caches.itervalues():
            cache.connection_pool.disconnect()

        if self._cache_cluster:
            self._cache_cluster.connection_pool.disconnect()
            self._cache_cluster = None

    def get_cache(self, name):
        host = self._cache_hashs.get_host(name)
        if host in self._caches: return self._caches[host]

        h, p = host.split(":") if ":" in host else (host, 6379)
        cache = Redis(host = h, port = int(p), socket_timeout = __conf__.SOCK_TIMEOUT,password=__conf__.CACHE_PASSWORD)
        self._caches[host] = cache

        return cache


    def get_redis_cluster(self):
        import redis
        from rediscluster import StrictRedisCluster
        serverip='101.200.214.57'
        startup_nodes=[{"host": serverip,"port": i} for i in xrange(7000, 7006)]
        self._cache_cluster = StrictRedisCluster(startup_nodes=startup_nodes)
        return self._cache_cluster


    def get_mq(self, name):
        return self.get_cache(name)


    @classmethod
    def get_mongoclient(cls, dbhost = None, dbname = None):
        if not dbhost:
            dbhost = dbhost or __conf__.DB_HOST

        if not dbname:
            dbname = dbname or __conf__.DB_NAME

        _attr = '__mongoclient__' + dbhost
        if not hasattr(cls, _attr):
            setattr(cls, _attr, pymongo.MongoClient(host = dbhost, socketTimeoutMS= __conf__.SOCK_TIMEOUT))

        return getattr(cls, _attr)[dbname]

    @classmethod
    def get_mysql(cls, host, user, passwd, db):
        import pymysql
        return pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset='utf8')
        _attr = '__mssql__' + host

        if not hasattr(cls, _attr):
            setattr(cls, _attr, pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset='utf8'))

        return getattr(cls, _attr)

    def get_gfs(self, dbhost = None, dbname = None):
        if not dbname:
            dbname = dbname or __conf__.GFS_NAME

        return GridFS(self.get_mongoclient(dbhost = dbhost, dbname = dbname))


    @classmethod
    def get_context(cls):
        """
            获取当前线程上下文对象，支持嵌套。
        """
        return hasattr(cls._thread_local, "contexts") and cls._thread_local.contexts and \
            cls._thread_local.contexts[-1] or None



get_context = LogicContext.get_context



__all__ = ["ConsistentHash", "LogicContext", "get_context"] 

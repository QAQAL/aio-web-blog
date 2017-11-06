import logging
import asyncio, os, json, time, aiomysql

from datetime import datetime
from aiohttp import web

logging.basicConfig(level=logging.INFO)


@asyncio.coroutine
def close_pool():
    global __pool
    __pool.close()
    yield from __pool.wait_closed()


@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    logging.log(sql, args)
    global __pool
    with (yield from __pool) as conn: # 从连接池中获取一个connect
        cur = yield from conn.cursor(aiomysql.DictCursor) # 获取游标cursor
        yield from cur.execute(sql.replace('?', '%'), args or ()) # 将输入的sql语句中的'?'，替换为具体参数args
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: {}'.format(len(rs)))
        return rs


@asyncio.coroutine
def execute(sql, args):
    logging.log(sql, args)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '{}'.format(args)))
            affected = cur.rowcount
            yield from cur.close
        except BaseException as e:
            raise
        return affected


def create_args_string(num):
    L = []
    for i in range(num):
        L.append('?')
    return ', '.join(L)


class Field(object):
    """
    用于表识model每个成员变量的类
    """
    def __init__(self, name, column_type, primary_key, default):
        self.name = name # 表名称
        self.column_type = column_type # 值类型
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<{}, {}: {}>'.format(self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, ddl='boolean', default=False, primary_key=False):
        super(BooleanField, self).__init__(name, ddl, primary_key, default)


class IntegerField(Field):
    def __init__(self, name=None, ddl='bigint', default=None, primary_key=None):
        super(IntegerField, self).__init__(name, ddl, primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, ddl='real', default=None, primary_key=None):
        super(FloatField, self).__init__(name, ddl, primary_key, default)


class TextField(Field):
    def __init__(self, name=None, ddl='text', default=None, primary_key=None):
        super(TextField, self).__init__(name, ddl, primary_key, default)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Medol类本身：
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: {} (table: {})'.format(name, tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: {}==> {}'.format(k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: {}'.format(k))
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found')
        for k in mappings.keys():
            attrs.pop(k) # 删除attrs里的属性，防止与实例属性发生冲突
        escaped_fields = list(map(lambda f: '`{}`'.format(f), fields))
        attrs['__mappings__'] = mappings # 保存属性和列表的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的SELECT，INSERT, UPDATTE和DELETE语句
        attrs['__select__'] = 'select `{}`, {} from `{}`'.format(primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `{}` ({}, `{}`) values ({})'.format(tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))
        attrs['__update__'] = 'update `{}` set {} where `{}`=?'.format(tableName, ','.join(map(lambda f:'`{}`=?'.format(mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `{}` where `{}`=?'.format(tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute'{}'".format(key))

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for {}: {}'.format(key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        sql = [cls.__select__]
        if where:
            sql.append('WHERE')
            sql.append(where)

        if args is None:
            args = []

        orderby = kw.get('orderby', None)
        if orderby:
            sql.append('ORDER BY')
            sql.append(orderby)

        limit = kw.get('LIMIT', None)
        if limit:
            sql.append('LIMIT')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple):
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: {}'.format(str(limit)))

        rs = yield from select(' '.join(sql), args)
        # 将每条记录作为对象返回
        return [cls(**r) for r in rs]

    # 过滤结果数量
    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        sql = ['SELECT {} _num_ from `{}`'.format(selectField, cls.__table__)]
        if where:
            sql.append('WHERE')
            sql.append(where)

        rs = yield from select(' '.join(sql), args)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    # 返回主键的一条记录
    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        'find object by primary key'
        rs = yield from select('{} where `{}`=?'.format((cls.__select__, cls.__primary_key__), [pk], 1))
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # INSERT command
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: {}'.format(rows))

    # UPDATE command
    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)
        if rows != 1:
            logging.warning('Faield to update by primary_key:affected rows: {}'.format(rows))

    # DELETE command
    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__delete__, args)
        if rows != 1:
            logging.warning('Failed to remove by primary key: affected {}'.format(rows))


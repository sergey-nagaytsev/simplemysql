#!/usr/bin/env python
# vim: fileencoding=utf-8: noexpandtab

"""
    A very simple wrapper for MySQLdb

    Methods:
        getOne() - get a single row
        getAll() - get all rows
        lastId() - get the last insert id
        lastQuery() - get the last executed query
        insert() - insert a row
        insertBatch() - Batch Insert
        insertOrUpdate() - insert a row or update it if it exists
        update() - update rows
        delete() - delete rows
        query()  - run a raw sql query
        leftJoin() - do an inner left join query and get results

    License: GNU GPLv2

    Kailash Nadh, http://nadh.in
    May 2013
"""

from collections import namedtuple
from itertools import repeat

import logging
import sys


def connect(deferred_connect, extra_calls=None):
    extra_calls = extra_calls or {}

    def _connect_inner():
        conn = deferred_connect()
        for method, args in extra_calls.items():
            if _has_method(conn, method):
                getattr(conn, method)(*args[0], **args[1])
        return conn

    return _connect_inner


def defer(fn, *args, **kwargs):
    def _deferred_inner():
        return fn(*args, **kwargs)

    return _deferred_inner


def func_args(*args, **kwargs):
    return args, kwargs


def _has_method(obj, name):
    return hasattr(obj, name) and callable(getattr(obj, name))


def _merge_dicts(*args):
    r = {}
    for d in args:
        r.update(d)
    return r


class Dialect():
    ordered_parameter = '%s'
    name_quote = '`'
    def can_reconnect(self, e):
        return True

    def quote_names(self, *args):
        q = self.name_quote
        r = [q + s + q for s in args]
        return r if len(r) > 1 else r[0]


class DialectMySQL(Dialect):

    def can_reconnect(self, e):
        return (e.__class__.__name__ in ['OperationalError']) and (2006 == e[0])


class DialectPostgres(Dialect):
    name_quote = '"'


class DialectSQLite3(Dialect):
    ordered_parameter = '?'


class SimpleMysql:
    conn = None
    cur = None
    conf = None

    def __init__(self, connection_factory, dialect=None, logger=None):
        self.dialect = dialect or DialectMySQL()
        if not logger:
            logger = logging.getLogger()
            logger.addHandler(logging.StreamHandler(sys.stderr))
        self.logger = logger
        self.connection_factory = connection_factory
        self.connect()

    def connect(self):
        """Connect to the mysql server"""

        try:
            self.conn = self.connection_factory()
            self.cur = self.conn.cursor()
        except Exception as e:
            self.logger.error('MySQL connection failed: %s', e) #_merge_dicts(self.conf, dict(passwd='***')))
            raise

    def getOne(self, table=None, fields='*', where=None, order=None, limit=(0, 1)):
        """Get a single result

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchone()

        row = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            row = Row(*result)

        return row

    def getAll(self, table=None, fields='*', where=None, order=None, limit=None):
        """Get all results

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def lastId(self):
        """Get the last insert id"""
        return self.cur.lastrowid

    def lastQuery(self):
        """Get the last executed query"""
        try:
            return self.cur.statement
        except AttributeError:
            return self.cur._last_executed

    def leftJoin(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query

            tables = (table1, table2)
            fields = ([fields from table1], [fields from table 2])  # fields to select
            join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """

        cur = self._select_join(tables, fields, join_fields, where, order, limit)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def insert(self, table, data):
        """Insert a record"""

        query = self._serialize_insert(data)

        sql = "INSERT INTO %s (%s) VALUES(%s)" % (table, query[0], query[1])

        return self.query(sql, data.values()).rowcount

    def insertBatch(self, table, data):
        """Insert multiple record"""

        query = self._serialize_batch_insert(data)
        sql = "INSERT INTO %s (%s) VALUES %s" % (table, query[0], query[1])
        flattened_values = [v for sublist in data for k, v in sublist.iteritems()]
        return self.query(sql, flattened_values).rowcount

    def update(self, table, data, where=None):
        """Insert a record"""

        query = self._serialize_update(data)

        sql = "UPDATE %s SET %s" % (table, query)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        return self.query(sql, data.values() + where[1] if where and len(where) > 1 else data.values()
                          ).rowcount

    def insertOrUpdate(self, table, data, keys):
        insert_data = data.copy()

        data = {k: data[k] for k in data if k not in keys}

        insert = self._serialize_insert(insert_data)

        update = self._serialize_update(data)

        sql = "INSERT INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (table, insert[0], insert[1], update)

        return self.query(sql, insert_data.values() + data.values()).rowcount

    def delete(self, table, where=None):
        """Delete rows based on a where condition"""

        sql = "DELETE FROM %s" % table

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        return self.query(sql, where[1] if where and len(where) > 1 else None).rowcount

    def query(self, sql, params=None):
        """Run a raw query"""

        sql = sql.replace('%s', self.dialect.ordered_parameter)

        # check if connection is alive. if not, reconnect
        try:
            self.logger.debug('SQL: "%s", params: %s', sql, params)
            self.cur.execute(sql, params)
        except Exception as e:
            # mysql timed out. reconnect and retry once
            if self.dialect.can_reconnect(e):
                self.connect()
                try:
                    self.cur.execute(sql, params)
                except Exception as e:
                    self.logger.error('Query error: "%s", SQL: "%s", params: %s', e, sql, params)
                    raise
            else:
                self.logger.error('Query error: "%s", SQL: "%s", params: %s', e, sql, params)
                raise

        return self.cur

    def commit(self):
        """Commit a transaction (transactional engines like InnoDB require this)"""
        return self.conn.commit()

    def is_open(self):
        """Check if the connection is open"""
        return self.conn.open

    def end(self):
        """Kill the connection"""
        self.cur.close()
        self.conn.close()

    # ===

    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])

        return [keys, vals]

    def _serialize_batch_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(data[0].keys())
        v = "(%s)" % ",".join(tuple("%s".rstrip(',') for v in range(len(data[0]))))
        l = ','.join(list(repeat(v, len(data))))
        return [keys, l]

    def _serialize_limit(self, limit):
        limit = [int(n) for n in limit]
        r = ''
        if 2 == len(limit):
            r = ' LIMIT %s OFFSET %s' % (limit[1], limit[0])
        elif 1 == len(limit):
            r = ' LIMIT %s' % limit[0]
        return r

    def _serialize_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _select(self, table=None, fields=(), where=None, order=None, limit=None):
        """Run a select query"""

        sql = "SELECT %s FROM %s" % (",".join(fields), self.dialect.quote_names(table))

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " %s" % order[1]

        # limit
        if limit:
            sql += self._serialize_limit(limit)

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def _select_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query"""

        fields = [tables[0] + "." + f for f in fields[0]] + \
                 [tables[1] + "." + f for f in fields[1]]

        sql = "SELECT %s FROM %s LEFT JOIN %s ON (%s = %s)" % \
              (",".join(fields),
               tables[0],
               tables[1],
               tables[0] + "." + join_fields[0], \
               tables[1] + "." + join_fields[1]
               )

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " " + order[1]

        # limit
        if limit:
            sql += self._serialize_limit(limit)

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.end()

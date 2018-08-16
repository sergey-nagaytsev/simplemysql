import unittest

import MySQLdb

import all_dialects

from simplemysql import SimpleMysql, connect, defer, func_args

_CONNECT = dict(
    host='127.0.0.1',
    port=3306,
    user='root',
    passwd='',
    db='test',
)


class MysqlTest(all_dialects.AllDialects):

    @classmethod
    def setUpClass(cls):
        cls.simplemysql = SimpleMysql(connect(defer(MySQLdb.connect,**_CONNECT),dict(autocommit=func_args(True))))

    def setUp(self):
        self.simplemysql.cur.execute('''
            CREATE TABLE IF NOT EXISTS `test` (
              `id` int(6) unsigned NOT NULL,
              `name` varchar(200) NOT NULL,
              PRIMARY KEY (`id`)
            ) DEFAULT CHARSET=utf8;
        ''')
        self.simplemysql.cur.execute('''
            INSERT INTO `test` (`id`, `name`) VALUES
              ('1', 'Value 1'),
              ('2', 'Value 2'),
              ('3', 'Do not look at me')
            ;
        ''')


if __name__ == '__main__':
    unittest.main()

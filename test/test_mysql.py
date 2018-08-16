import unittest

import MySQLdb

from simplemysql import SimpleMysql, connect, defer, func_args

_CONNECT = dict(
    host='127.0.0.1',
    port=3306,
    user='root',
    passwd='',
    db='test',
)


class MysqlTest(unittest.TestCase):
    simplemysql = None

    @classmethod
    def setUpClass(cls):
        cls.simplemysql = SimpleMysql(connect(defer(MySQLdb.connect,**_CONNECT),dict(autocommit=func_args(True))))

    @classmethod
    def tearDownClass(cls):
        cls.simplemysql.end()

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

    def tearDown(self):
        self.simplemysql.cur.execute('DROP TABLE `test`;')

    def testGetOne(self):
        row = self.simplemysql.getOne('test', where=("id=%s", [2]))
        self.assertEqual(row.name, 'Value 2')

    def testGetAll(self):
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%Value%']))  # , order=('name', 'ASC'))
        self.assertEqual(len(rowset), 2)


if __name__ == '__main__':
    unittest.main()

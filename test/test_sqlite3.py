import sqlite3

from . import all_dialects
from simplemysql import DialectSQLite3, SimpleMysql


class SQLite3Test(all_dialects.AllDialects):

    @classmethod
    def setUpClass(cls):
        cls.simplemysql = SimpleMysql(lambda : sqlite3.connect(':memory:'), DialectSQLite3())

    def setUp(self):
        self.simplemysql.cur.execute('''
            CREATE TABLE IF NOT EXISTS `test` (
              `id` INTEGER PRIMARY KEY AUTOINCREMENT,
              `name` varchar(200) NOT NULL
            );
        ''')
        self.simplemysql.cur.execute('''
            INSERT INTO `test` (`id`, `name`) VALUES
              ('1', 'Value 1'),
              ('2', 'Value 2'),
              ('3', 'Do not look at me')
            ;
        ''')

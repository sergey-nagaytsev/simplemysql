import psycopg2

from . import all_dialects
from simplemysql import DialectPostgres, SimpleMysql, defer

_CONNECT = dict(
    host='127.0.0.1',
    port=5432,
    user='travis',
    password='',
    dbname='test',
)


class PostgresTest(all_dialects.AllDialects):

    @classmethod
    def setUpClass(cls):
        cls.simplemysql = SimpleMysql(defer(psycopg2.connect, **_CONNECT), DialectPostgres())

    def setUp(self):
        self.simplemysql.cur.execute('''
            CREATE TABLE IF NOT EXISTS "test" (
              "id" int NOT NULL,
              "name" varchar NOT NULL,
              PRIMARY KEY ("id")
            );
        ''')
        self.simplemysql.cur.execute('''
            INSERT INTO "test" ("id", "name") VALUES
              ('1', 'Value 1'),
              ('2', 'Value 2'),
              ('3', 'Do not look at me')
            ;
        ''')

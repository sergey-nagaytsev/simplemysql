import psycopg2

from . import all_dialects
from simplemysql import DialectPostgres, SimpleMysql


def _connection_factory():
    conn = psycopg2.connect(
        host='127.0.0.1',
        port=5432,
        user='travis',
        password='',
        dbname='test',
    )
    conn.autocommit = True
    return conn


class PostgresTest(all_dialects.AllDialects):

    @classmethod
    def setUpClass(cls):
        cls.simplemysql = SimpleMysql(_connection_factory, DialectPostgres())

    def setUp(self):
        self.simplemysql.cur.execute('''
            CREATE TABLE IF NOT EXISTS "test" (
              "id" SERIAL,
              "name" varchar NOT NULL,
              PRIMARY KEY ("id")
            );
        ''')
        self.simplemysql.cur.execute('''
        ALTER SEQUENCE test_id_seq RESTART WITH 4;
        ''')
        self.simplemysql.cur.execute('''
            INSERT INTO "test" ("id", "name") VALUES
              ('1', 'Value 1'),
              ('2', 'Value 2'),
              ('3', 'Do not look at me')
            ;
        ''')

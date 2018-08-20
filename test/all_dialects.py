import unittest


class AllDialects(unittest.TestCase):
    simplemysql = None

    @classmethod
    def tearDownClass(cls):
        cls.simplemysql.end()

    def tearDown(self):
        self.simplemysql.cur.execute('DROP TABLE IF EXISTS %s;' % self.simplemysql.dialect.quote_names('test') )

    def testGetOne(self):
        row = self.simplemysql.getOne('test', where=("id=%s", [2]))
        self.assertEqual(row.name, 'Value 2')

    def testGetAll(self):
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%Value%']))  # , order=('name', 'ASC'))
        self.assertEqual(len(rowset), 2)

    def testGetAllOrder(self):
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%Value%']), order=('name', 'ASC'))
        self.assertEqual(rowset[0].name, 'Value 1')

    def testInsert(self):
        self.simplemysql.insert('test', dict(name='Firefox'))
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%fox%']))
        self.assertEqual(1, len(rowset))

    def testInsertBatch(self):
        self.simplemysql.insertBatch('test', [
            dict(name='Apache Cassandra'),
            dict(name='Apache Kafka'),
        ])
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%Apache%']))
        self.assertEqual(2, len(rowset))

    def testUpdate(self):
        self.simplemysql.update('test', dict(name='Value I'), ('id=%s', [1]))
        rowset = self.simplemysql.getAll('test', where=("name = %s", ['Value I']))
        self.assertEqual(1, len(rowset))

    def testInsertOrUpdate(self):
        self.simplemysql.insertOrUpdate('test', dict(id='2', name='Value II'), ['id'])
        rowset = self.simplemysql.getAll('test', where=("name = %s", ['Value II']))
        self.assertEqual(1, len(rowset))

    def testDelete(self):
        self.simplemysql.delete('test', ('name LIKE %s', ['%Do not look%']))
        rowset = self.simplemysql.getAll('test', where=("id = %s", [3]))
        self.assertEqual(rowset, None)

    def testQuery(self):
        cur = self.simplemysql.query('SELECT SUM(id) AS s FROM test;', [])
        self.assertEqual(int(cur.fetchall()[0][0]), 6)

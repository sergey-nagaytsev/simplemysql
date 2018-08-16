import unittest


class AllDialects(unittest.TestCase):
    simplemysql = None

    @classmethod
    def tearDownClass(cls):
        cls.simplemysql.end()

    def tearDown(self):
        self.simplemysql.cur.execute('DROP TABLE IF EXISTS `test`;')

    def testGetOne(self):
        row = self.simplemysql.getOne('test', where=("id=%s", [2]))
        self.assertEqual(row.name, 'Value 2')

    def testGetAll(self):
        rowset = self.simplemysql.getAll('test', where=("name LIKE %s", ['%Value%']))  # , order=('name', 'ASC'))
        self.assertEqual(len(rowset), 2)

import unittest
from udbp.Models.SQLiteModel import SQLiteModel

class TestSQLiteModel(unittest.TestCase):
    def setUp(self):
        class TestModel(SQLiteModel):
            __fields__ = {
                'id': 'Integer',
                'name': 'String',
                'age': 'Integer',
                'height': 'Float'
            }
            __tablename__ = 'test_table'

        self.TestModel = TestModel

    def test_create_table(self):
        expected_sql = (
            "CREATE TABLE IF NOT EXISTS test_table "
            "(id INTEGER, name TEXT, age INTEGER, height REAL)"
        )
        self.assertEqual(self.TestModel.create_table(), expected_sql)

    def test_to_dict(self):
        instance = self.TestModel(id=1, name='John', age=30, height=1.75)
        expected_dict = {'id': 1, 'name': 'John', 'age': 30, 'height': 1.75}
        self.assertEqual(instance.to_dict(), expected_dict)

    def test_from_dict(self):
        data = {'id': 1, 'name': 'John', 'age': 30, 'height': 1.75}
        instance = self.TestModel.from_dict(data)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.name, 'John')
        self.assertEqual(instance.age, 30)
        self.assertEqual(instance.height, 1.75)

    def test_get_insert_sql(self):
        instance = self.TestModel(id=1, name='John', age=30, height=1.75)
        sql, params = instance.get_insert_sql()
        expected_sql = "INSERT INTO test_table (id, name, age, height) VALUES (?, ?, ?, ?)"
        expected_params = (1, 'John', 30, 1.75)
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_get_select_sql(self):
        filters = {'name': 'John', 'age': 30}
        sql, params = self.TestModel.get_select_sql(filters)
        expected_sql = "SELECT * FROM test_table WHERE name = ? AND age = ?"
        expected_params = ('John', 30)
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, expected_params)

    def test_from_db_row(self):
        row = (1, 'John', 30, 1.75)
        instance = self.TestModel.from_db_row(row)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.name, 'John')
        self.assertEqual(instance.age, 30)
        self.assertEqual(instance.height, 1.75)

if __name__ == '__main__':
    unittest.main()

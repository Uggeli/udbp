import unittest
import sqlite3
from unittest.mock import patch, MagicMock
from udbp.Handlers.SQLiteHandler import SQLiteHandler

class TestSQLiteHandler(unittest.TestCase):
    def setUp(self):
        self.config = {'db_name': 'test.db'}
        self.handler = SQLiteHandler('test.db', self.config)

    @patch('sqlite3.connect')
    def test_initialize(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.handler.initialize()

        mock_connect.assert_called_once_with('./sqlitedbs/test.db', check_same_thread=False)
        mock_cursor.execute.assert_called_once_with(
            'CREATE TABLE IF NOT EXISTS _models (name TEXT PRIMARY KEY, fields TEXT NOT NULL)'
        )
        mock_connection.commit.assert_called_once()

    @patch('sqlite3.connect')
    def test_create_model(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.handler.initialize()
        fields = {'name': 'String', 'age': 'Integer'}
        model_class = self.handler.create_model('TestModel', fields)

        self.assertEqual(model_class.__name__, 'TestModel')
        self.assertEqual(model_class.__fields__, fields)
        self.assertEqual(model_class.__tablename__, 'TestModel')

        mock_cursor.execute.assert_any_call(
            'INSERT OR REPLACE INTO _models (name, fields) VALUES (?, ?)',
            ('TestModel', '{"name": "String", "age": "Integer"}')
        )

    @patch('sqlite3.connect')
    def test_store_data(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.handler.initialize()
        fields = {'name': 'String', 'age': 'Integer'}
        self.handler.create_model('TestModel', fields)

        data = {'name': 'John', 'age': 30}
        self.handler.store_data('TestModel', data)

        mock_cursor.execute.assert_called_with(
            'INSERT INTO TestModel (name, age) VALUES (?, ?)',
            ('John', 30)
        )

    @patch('sqlite3.connect')
    def test_retrieve_data(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.handler.initialize()
        fields = {'name': 'String', 'age': 'Integer'}
        self.handler.create_model('TestModel', fields)

        mock_cursor.fetchall.return_value = [('John', 30), ('Jane', 25)]

        filters = {'age': 30}
        result = self.handler.retrieve_data('TestModel', filters)

        mock_cursor.execute.assert_called_with(
            'SELECT * FROM TestModel WHERE age = ?',
            (30,)
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'John')
        self.assertEqual(result[0].age, 30)

if __name__ == '__main__':
    unittest.main()

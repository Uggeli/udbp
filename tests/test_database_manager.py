import unittest
import asyncio
from unittest.mock import patch, MagicMock
from udbp.DatabaseManager import DatabaseManager

class TestDatabaseManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'sqlite': {
                'max_connections': 5
            },
            'max_workers': 10
        }
        self.manager = DatabaseManager(self.config)

    def test_get_handler(self):
        db_name = 'test_db'
        db_type = 'sqlite'
        handler = self.manager.get_handler(db_name, db_type)
        self.assertIsNotNone(handler)
        self.assertEqual(handler.db_name, db_name)

    @patch('udbp.DatabaseManager.SQLiteHandler')
    async def test_execute_operation(self, mock_handler_class):
        mock_handler = MagicMock()
        mock_handler_class.return_value = mock_handler

        db_name = 'test_db'
        db_type = 'sqlite'
        operation = 'create_model'
        kwargs = {'name': 'TestModel', 'fields': {'name': 'String', 'age': 'Integer'}}

        await self.manager.execute_operation(db_name, db_type, operation, **kwargs)

        mock_handler_class.assert_called_once_with(db_name, self.config['sqlite'])
        mock_handler.initialize.assert_called_once()
        getattr(mock_handler, operation).assert_called_once_with(**kwargs)

    @patch('udbp.DatabaseManager.DatabaseManager.execute_operation')
    async def test_create_model(self, mock_execute):
        db_name = 'test_db'
        db_type = 'sqlite'
        model_name = 'TestModel'
        fields = {'name': 'String', 'age': 'Integer'}

        await self.manager.create_model(db_name, db_type, model_name, fields)

        mock_execute.assert_called_once_with(
            db_name, db_type, 'create_model', name=model_name, fields=fields
        )

    @patch('udbp.DatabaseManager.DatabaseManager.execute_operation')
    async def test_store_data(self, mock_execute):
        db_name = 'test_db'
        db_type = 'sqlite'
        model_name = 'TestModel'
        data = {'name': 'John', 'age': 30}

        await self.manager.store_data(db_name, db_type, model_name, data)

        mock_execute.assert_called_once_with(
            db_name, db_type, 'store_data', model_name=model_name, data=data
        )

    @patch('udbp.DatabaseManager.DatabaseManager.execute_operation')
    async def test_retrieve_data(self, mock_execute):
        db_name = 'test_db'
        db_type = 'sqlite'
        model_name = 'TestModel'
        filters = {'age': 30}

        await self.manager.retrieve_data(db_name, db_type, model_name, filters)

        mock_execute.assert_called_once_with(
            db_name, db_type, 'retrieve_data', model_name=model_name, filters=filters
        )

if __name__ == '__main__':
    unittest.main()
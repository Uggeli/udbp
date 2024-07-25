import unittest
import asyncio
import os
from udbp.DatabaseManager import DatabaseManager
from udbp.config import SQLITE_PATH

class TestDatabaseIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = {
            'sqlite': {
                'max_connections': 5
            },
            'max_workers': 10
        }
        self.db_name = 'test_integration_db'
        self.db_type = 'sqlite'
        self.manager = DatabaseManager(self.config)

    async def asyncTearDown(self):
        await self.manager.shutdown()
        # Remove the test database file
        try:
            os.remove(f'{SQLITE_PATH}{self.db_name}.db')
        except FileNotFoundError:
            pass

    async def test_full_cycle(self):
        # 1. Create a model
        user_model_name = 'User'
        user_fields = {
            'id': 'Integer',
            'name': 'String',
            'age': 'Integer',
            'email': 'String'
        }
        await self.manager.create_model(self.db_name, self.db_type, user_model_name, user_fields)

        # 2. Verify the model was created
        handler = self.manager.get_handler(self.db_name, self.db_type)
        models = handler.get_models()
        self.assertIn(user_model_name, models)
        
        # 3. Store data
        user_data = [
            {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'},
            {'name': 'Bob', 'age': 25, 'email': 'bob@example.com'},
            {'name': 'Charlie', 'age': 35, 'email': 'charlie@example.com'}
        ]
        for data in user_data:
            await self.manager.store_data(self.db_name, self.db_type, user_model_name, data)

        # 4. Retrieve and verify data
        all_users = await self.manager.retrieve_data(self.db_name, self.db_type, user_model_name)
        self.assertEqual(len(all_users), 3)
        
        for i, user in enumerate(all_users):
            self.assertEqual(user.name, user_data[i]['name'])
            self.assertEqual(user.age, user_data[i]['age'])
            self.assertEqual(user.email, user_data[i]['email'])

        # 5. Test filtering
        young_users = await self.manager.retrieve_data(self.db_name, self.db_type, user_model_name, {'age': 25})
        self.assertEqual(len(young_users), 1)
        self.assertEqual(young_users[0].name, 'Bob')

        # 6. Create another model with a foreign key
        post_model_name = 'Post'
        post_fields = {
            'id': 'Integer',
            'title': 'String',
            'content': 'String',
            'user_id': 'Integer'  # Foreign key to User
        }
        await self.manager.create_model(self.db_name, self.db_type, post_model_name, post_fields)

        # 7. Verify the new model was created
        models = handler.get_models()
        self.assertIn(post_model_name, models)

        # 8. Store data in the new model
        post_data = [
            {'title': 'First Post', 'content': 'Hello, World!', 'user_id': 1},
            {'title': 'Second Post', 'content': 'Another post', 'user_id': 2}
        ]
        for data in post_data:
            await self.manager.store_data(self.db_name, self.db_type, post_model_name, data)

        # 9. Retrieve and verify data from the new model
        all_posts = await self.manager.retrieve_data(self.db_name, self.db_type, post_model_name)
        self.assertEqual(len(all_posts), 2)
        
        for i, post in enumerate(all_posts):
            self.assertEqual(post.title, post_data[i]['title'])
            self.assertEqual(post.content, post_data[i]['content'])
            self.assertEqual(post.user_id, post_data[i]['user_id'])

if __name__ == '__main__':
    unittest.main()
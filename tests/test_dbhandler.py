import sys
import os

import requests

sys.path.insert(0, os.getcwd())

from udbp.plugins.dbhandler_plugin import DatabaseHandler  # noqa: E402
from udbp.models.testdb.testdataclass import User, Order  # noqa: E402


def setup_logger():
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def test_model():
    user = User(id=1, name='John Doe', email='test@test')
    order = Order(id=1, user_id=1, order_date='2021-01-01')

    # Test dataclass attributes
    assert user.id == 1
    assert user.name == 'John Doe'
    assert user.email == 'test@test'

    assert order.id == 1
    assert order.user_id == 1
    assert order.order_date == '2021-01-01'

    # Test create_table method
    assert user.create_table() == 'CREATE TABLE IF NOT EXISTS "User" ("id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "name" TEXT NOT NULL, "email" TEXT NOT NULL)'
    assert order.create_table() == 'CREATE TABLE IF NOT EXISTS "Order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "user_id" INTEGER NOT NULL, "order_date" TEXT NOT NULL, FOREIGN KEY("user_id") REFERENCES "user"("id") ON DELETE CASCADE)'

    # Test create_insert method
    print(user.create_insert())
    assert user.create_insert() == ('INSERT INTO "User" ("name", "email") VALUES (:name, :email)',
                                    {'id': 1, 'name': 'John Doe', 'email': 'test@test'})
    assert order.create_insert() == ('INSERT INTO "Order" ("user_id", "order_date") VALUES (:user_id, :order_date)',
                                     {'id': 1, 'user_id': 1, 'order_date': '2021-01-01'})


def test_dbhandler():
    db = DatabaseHandler('testdb', 'sqlite')

    assert db.dbname == 'testdb'
    assert db.dbtype == 'sqlite'
    assert db.models == {'User': User, 'Order': Order}
    assert db.connection is not None

    db.store_data('User', {'id': 1, 'name': 'John Doe', 'email': 'test@test'})
    db.store_data('Order', {'user_id': 1, 'order_date': '2021-01-01'})
    db.store_data('Order', {'user_id': 1, 'order_date': '2021-01-02'})
    db.store_data('Order', {'user_id': 1, 'order_date': '2021-01-03'})


def test_endpoints():
    data = {
        'dbname': 'testdb',
        'dbtype': 'sqlite',
        'datatype': 'User',
        'data': {'id': 1, 'name': 'John Doe', 'email': 'test@test'}
    }
    response = requests.post('http://localhost:5000/store', json=data)
    assert response.status_code == 200


if __name__ == '__main__':
    setup_logger()
    test_model()
    test_dbhandler()
    test_endpoints()

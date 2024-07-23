import sys
import os

import requests
import requests

sys.path.insert(0, os.getcwd())


def test_connection_endpoint():
    response = requests.post('http://localhost:5000/connect', json={
        'dbname': 'test_connection',
        'dbtype': 'sqlite',
        'db_models': {
            'user': {
                'name': 'String',
                'age': 'Integer',
                'address': 'addressData'
            },
            'addressData': {
                'street': 'String',
                'city': 'String',
                'zip': 'String'
            },
        },
    })

    assert response.status_code == 200
    assert response.json() == {'status': 'success'}
    print('Test passed')

def  test_store_endpoint():
    response = requests.post('http://localhost:5000/store', json={
        'dbname': 'test_connection',
        'dbtype': 'sqlite',
        'model': 'user',
        'data': {
            'name': 'John Doe',
            'age': 30,
            'address': {
                'street': '123 Main St',
                'city': 'Springfield',
                'zip': '12345'
            }
        }
    })

    assert response.status_code == 200
    assert response.json() == {'status': 'success'}
    print('Test passed')


if __name__ == '__main__':
    test_connection_endpoint()
    test_store_endpoint()

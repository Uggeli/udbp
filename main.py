import os
import sys

sys.path.insert(0, os.getcwd())

from flask import Flask, request, jsonify
import logging
from udbp.plugins.dbhandler_plugin import DatabaseHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
app = Flask('udbp')
app.config['DEBUG'] = True

active_databases = {}

@app.route('/connect', methods=['POST'])
def connect():
    """Crawler checks in with the server and gets a unique ID, also sends schema information"""
    try:
        data = request.get_json()
        dbname = data['dbname']
        dbtype = data['dbtype']
        db_models = data['db_models']
        logger.info(f'Connecting to database: {dbname} - {dbtype} - {db_models}')
        if dbname not in active_databases:
            active_databases[dbname] = DatabaseHandler(dbname, dbtype,model_data=db_models)
        # else:
        #     active_databases[dbname].create_schema(db_models)

        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error connecting to database: {e}')
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/store', methods=['POST'])
def store():
    try:
        data = request.get_json()
        logger.info(f'Storing data: {data}')
        if data['dbname'] not in active_databases:
            active_databases[data['dbname']] = DatabaseHandler(data['dbname'], data['dbtype'])
        active_databases[data['dbname']].store_data(data['model'], data['data'])
        # logger.info(f'Data stored successfully: {data}')
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error storing data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/bulk_store', methods=['POST'])
def bulk_store():
    try:
        data = request.get_json()
        logger.info(f'Storing data: {data}')
        if data['dbname'] not in active_databases:
            active_databases[data['dbname']] = DatabaseHandler(data['dbname'], data['dbtype'])
        for bulk in data['data']:
            active_databases[data['dbname']].store_data(data['model'], bulk)
        # logger.info(f'Data stored successfully: {data}')
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error storing data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/retrieve', methods=['POST'])
def retrieve():
    try:
        data = request.get_json()
        logger.info(f'Retrieving data: {data}')
        db = DatabaseHandler(data['dbname'], data['dbtype'])
        result = db.retrieve_data(data['model'], data['filters'])
        return jsonify({'status': 'success', 'data': result})
    except Exception as e:
        logger.error(f'Error retrieving data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})


if __name__ == '__main__':
    app.run(port=5000)

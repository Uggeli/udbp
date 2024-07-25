from flask import Flask, request, jsonify
import logging
from udbp.DatabaseManager import DatabaseManager


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
app = Flask('udbp')
app.config['DEBUG'] = True

config = {
    'sqlite': {
        'max_connections': 5
    },
    'max_workers': 10
}
database_manager = DatabaseManager(config)

@app.route('/connect', methods=['POST'])
async def connect():
    """Crawler checks in with the server and gets a unique ID, also sends schema information"""
    try:
        data = request.get_json()
        dbname = data['dbname']
        dbtype = data['dbtype']
        db_models = data['db_models']
        logger.info(f'Connecting to database: {dbname} - {dbtype} - {db_models}')
        
        for model_name, fields in db_models.items():
            await database_manager.create_model(dbname, dbtype, model_name, fields)

        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error connecting to database: {e}')
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/store', methods=['POST'])
async def store():
    try:
        data = request.get_json()
        logger.info(f'Storing data: {data}')
        await database_manager.store_data(data['dbname'], data['dbtype'], data['model'], data['data'])
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error storing data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/bulk_store', methods=['POST'])
async def bulk_store():
    try:
        data = request.get_json()
        logger.info(f'Storing bulk data: {data}')
        for item in data['data']:
            await database_manager.store_data(data['dbname'], data['dbtype'], data['model'], item)
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f'Error storing bulk data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/retrieve', methods=['POST'])
async def retrieve():
    try:
        data = request.get_json()
        logger.info(f'Retrieving data: {data}')
        result = await database_manager.retrieve_data(data['dbname'], data['dbtype'], data['model'], data['filters'])
        return jsonify({'status': 'success', 'data': result})
    except Exception as e:
        logger.error(f'Error retrieving data: {e}')
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(port=5000)
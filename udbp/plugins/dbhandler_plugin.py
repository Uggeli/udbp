import importlib
import inspect
import os
import logging
import threading

from dataclasses import dataclass
from queue import Queue

from udbp.config import SQLITE_PATH
from udbp.plugins.database_model import Model, DBfield, DBint, DBtext, DBreal, DBblob


logger = logging.getLogger(__name__)


class DBHandlingMethodsModule:
    def store_data_sqlite3(self, model: Model, connection):
        try:
            cursor = connection.cursor()
            insert_str, values = model.create_insert()
            cursor.execute(insert_str, values)
            connection.commit()
            inserted_id = cursor.lastrowid
            cursor.close()
            logger.info(f'Data stored successfully: {model}')
            return inserted_id
        except Exception as e:
            logger.error(f'Error storing data: {e}')
            raise

    def create_tables_sqlite3(self, connection, models: dict):
        try:
            cursor = connection.cursor()
            for model in models.values():
                try:
                    create_str = model.create_table('sqlite3')
                    cursor.execute(create_str)
                    logger.info(f'Table created successfully: {model.__name__}')
                except Exception as e:
                    logger.error(f'Error creating table: {e}')
            connection.commit()
            cursor.close()
            logger.info('Tables created successfully')
        except Exception as e:
            logger.error(f'Error creating tables: {e}')
        pass

    def retrieve_data_sqlite3(self, model: Model, connection, filters: dict = None):
        try:
            cursor = connection.cursor()
            query = f"SELECT * FROM {model.__name__}"
            params = ()
            if filters:
                conditions = " AND ".join([f"{key} = ?" for key in filters.keys()])
                query += f" WHERE {conditions}"
                params = tuple(filters.values())
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            return [model(**dict(zip(model._fields.keys(), row))) for row in results]
        except Exception as e:
            logger.error(f'Error retrieving data: {e}')
            raise

    def construct_query_sqlite3(self, model, query) -> str:
        command = f'SELECT * FROM {model.__class__.__name__}'
        if query:
            filters = ' AND '.join([f'{key}=?' for key in query.keys()])
            command += f' WHERE {filters}'
            params = tuple(query.values())
        else:
            params = ()
        return command, params

    def create_schema(self, db_models: list):
        try:
            if self.dbtype == 'sqlite':
                with self.sql_module.connect(f'{SQLITE_PATH}{self.dbname}.db') as connection:
                    cursor = connection.cursor()
                    model_classes = {}
                    
                    # First pass: create all model classes
                    for model_dict in db_models:
                        logger.debug(f'model dict: {model_dict}')
                        for model_name, fields in model_dict.items():
                            model_classes[model_name] = type(model_name, (Model,), {
                                field_name: self.create_db_field(field_type, model_classes)
                                for field_name, field_type in fields.items()
                            })
                    
                    # Second pass: create tables
                    for model_name, model_class in model_classes.items():
                        create_table_sql = model_class.create_table_sqlite3()
                        cursor.execute(create_table_sql)
                    
                    connection.commit()
            logger.info(f'Schema created successfully for {self.dbname}')
        except Exception as e:
            logger.error(f'Error creating schema: {e}')

    def create_db_field(self, field_type: str, model_classes: list) -> DBfield:
        if field_type in model_classes:
            # If the field type matches a model name, set it as a foreign key
            return DBint(foreign_key=(field_type, 'id'))
        elif field_type == 'String':
            return DBtext()
        elif field_type == 'Float':
            return DBreal()
        elif field_type == 'Integer':
            return DBint()
        elif field_type == 'Date':
            return DBtext()  # SQLite doesn't have a native DATE type
        elif field_type == 'Array':
            return DBtext()  # Store arrays as JSON strings
        elif field_type == 'Binary':
            return DBblob()
        else:
            return DBtext()  # Default to TEXT for unknown types
        
    def get_field_type(self, field_type: str, model_classes: list) -> str:
        if field_type in model_classes:
            return f'DBint = DBint(foreign_key=("{field_type}", "id"))'
        if field_type == 'String':
            return 'DBtext = DBtext()'
        elif field_type == 'Float':
            return 'DBreal = DBreal()'
        elif field_type == 'Integer':
            return 'DBint = DBint()'
        elif field_type == 'Date':
            return 'DBtext = DBtext()'  # SQLite doesn't have a native DATE type
        elif field_type == 'Array':
            return 'DBtext = DBtext()'  # Store arrays as JSON strings
        elif field_type == 'Binary':
            return 'DBblob = DBblob()'
        else:
            return 'DBtext = DBtext()'  # Default to TEXT for unknown types

@dataclass
class DatabaseOperation:
    operation: str  # 'insert', 'select', etc.
    query: str
    params: tuple
    callback: callable = None  # Optional callback to handle results


class DatabaseHandler(DBHandlingMethodsModule):
    _instances = {}

    def __new__(cls, dbname: str, dbtype: str, *args, **kwargs) -> 'DatabaseHandler':
        db = f'{dbname}_{dbtype}'
        lock = threading.Lock()
        with lock:
            if db not in cls._instances:
                cls._instances[db] = super().__new__(cls)
        return cls._instances[db]

    def __init__(self, dbname: str, dbtype: str, worker=False, modelpath: str = None, model_data: dict = None) -> None:
        if hasattr(self, 'initialized'):
            return
        self.dbname = dbname
        self.dbtype = dbtype
        self.modelpath = modelpath
        self.models = None
        try:
            self.init_database(model_data)
        except Exception as e:
            logger.error(f'Error initializing database handler: {e}')
            self.shutdown()
        if worker:
            try:
                self.db_queue = Queue()
                self.worker = threading.Thread(target=self.db_worker)
                self.worker.daemon = True
                self.worker.start()
                logger.info(f'Database worker started for {self.dbname}')
            except Exception as e:
                logger.error(f'Error initializing database worker: {e}')
        self.initialized = True

    def init_database(self, model_data: dict):
        try:
            self.load_models(model_data)
            if self.dbtype == 'sqlite':
                self.sql_module = importlib.import_module('sqlite3')
                if not os.path.exists(SQLITE_PATH):
                    os.makedirs(SQLITE_PATH)
                connection = self.sql_module.connect(f'{SQLITE_PATH}{self.dbname}.db')
                # Enable foreign key constraints
                connection.execute('PRAGMA foreign_keys = ON')
                # Initialize database
                self.create_tables_sqlite3(connection, self.models)
                logger.info(f'Database {self.dbname} initialized successfully')
            else:
                raise ValueError(f'Invalid database type: {self.dbtype}')
        except Exception as e:
            logger.error(f'Error initializing database: {e}')
            self.shutdown()

    def db_worker(self):
        while True:
            task = self.db_queue.get()
            if task:
                if task.operation == 'insert':
                    self.store_data(task.query, task.params)
                elif task.operation == 'select':
                    self.retrieve_data(task.query, task.params)
            self.db_queue.task_done()

    def add_task(self, operation: str, query: str, params: tuple, callback: callable = None):
        self.db_queue.put(DatabaseOperation(operation, query, params, callback))

    def load_models(self, model_data: dict):
        if not self.modelpath:
            self.modelpath = f'udbp.models.{self.dbname}'

        # check if the model exists
        if model_data and not os.path.exists(f'{self.modelpath}/{self.dbname}.py'):
            logger.error(f'Model file not found: {self.modelpath}')
            logger.error('attempting to create the files needed...')
            self.create_models(model_data)
        try:
            module = importlib.import_module(f'{self.modelpath}.{self.dbname}')
            models = {name: obj for name, obj in inspect.getmembers(module)
                      if inspect.isclass(obj) and issubclass(obj, Model) and name != 'Model'}
            if not models:
                raise ValueError('No data classes found')
            logger.info(f'Data classes loaded successfully: {", ".join([model for model in models])}')
            self.models = models
        except Exception as e:
            logger.error(f'Error loading data classes: {e}')
            logger.debug(f'Model path: {self.modelpath}')
            logger.debug(f'current working directory: {os.getcwd()}')
            raise

    def create_models(self, db_models: dict):
        try:
            logger.debug(f'Creating model file: {self.modelpath}')
            logger.debug(f'Model data: {db_models}')
            if not os.path.exists(f'{self.modelpath.replace(".", "/")}'):
                os.makedirs(f'{self.modelpath.replace(".", "/")}')

            with open(f'{self.modelpath.replace('.', '/')}/{self.dbname}.py', 'w') as model_file:
                model_file.write('"""This file was auto-generated by the udbp plugin"""\n\n')
                model_file.write('from udbp.plugins.database_model import Model, DBint, DBtext, DBreal, DBblob\n\n')
                for model_name, fields in db_models.items():
                    logger.debug(f'Creating model: {model_name}')
                    model_file.write(f'class {model_name}(Model):\n')
                    for field_name, field_type in fields.items():
                        logger.debug(f'Creating field: {field_name} - {field_type}')
                        model_file.write(f'    {field_name}: {self.get_field_type(field_type, db_models)}\n')
                    model_file.write('\n')
                    logger.debug(f'Model created successfully: {model_name}')
            if os.path.exists(f'{self.modelpath.replace(".", "/")}/{self.dbname}.py'):
                logger.info(f'Model file created successfully: {self.modelpath}')
            else:
                raise ValueError('Error creating model file')
        except Exception as e:
            logger.error(f'Error creating model file: {e}')

    def store_data(self, datatype: str, data: dict):
        if self.models and datatype in self.models:
            try:
                model = self.models[datatype]
                processed_data = {}
                for field_name, field_value in data.items():
                    if isinstance(field_value, dict) and field_name in model._fields:
                        # This is a nested structure
                        nested_model = self.models[model._fields[field_name].foreign_key[0]]
                        nested_id = self.store_data(nested_model.__name__, field_value)
                        processed_data[field_name] = nested_id
                    else:
                        processed_data[field_name] = field_value

                if self.dbtype == 'sqlite':
                    with self.sql_module.connect(f'{SQLITE_PATH}{self.dbname}.db') as connection:
                        return self.store_data_sqlite3(model(**processed_data), connection)
            except Exception as e:
                logger.error(f'Error storing data: {e}')

    def retrieve_data(self, datatype: str, filters: dict = None):
        if self.models and datatype in self.models:
            try:
                model = self.models[datatype]
                if self.dbtype == 'sqlite':
                    with self.sql_module.connect(f'{SQLITE_PATH}{self.dbname}.db') as connection:
                        data = self.retrieve_data_sqlite3(model, connection, filters)
                        
                        # Fetch related data
                        for item in data:
                            for field_name, field in model._fields.items():
                                if field.foreign_key:
                                    related_model_name, related_field = field.foreign_key
                                    related_id = getattr(item, f"{field_name}_id")
                                    related_data = self.retrieve_data(related_model_name, {"id": related_id})
                                    if related_data:
                                        setattr(item, field_name, related_data[0])
                        
                        return data
            except Exception as e:
                logger.error(f'Error retrieving data: {e}')
                raise
        return []
    
    def shutdown(self):
        logger.info(f'Shutting down database: {self.dbname} handler')
        if self.connection:
            self.connection.close()
        pass

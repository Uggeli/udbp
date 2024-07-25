import sqlite3
import json
from typing import Dict, Any, List, Type
from udbp.Models.SQLiteModel import SQLiteModel
from udbp.config import SQLITE_PATH

class SQLiteHandler:
    def __init__(self, db_name: str, config: Dict[str, Any]):
        self.db_name = db_name
        if self.db_name.endswith('.db'):
            self.db_name = self.db_name[:-3]
        self.config = config
        self.connection = None
        self.cursor = None
        self.models: Dict[str, Type[SQLiteModel]] = {}

    def initialize(self):
        self.connection = sqlite3.connect(f"{SQLITE_PATH}{self.db_name}.db", check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._create_models_table()

    def _create_models_table(self):
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS _models (name TEXT PRIMARY KEY, fields TEXT NOT NULL)'
        )
        self.connection.commit()

    def create_model(self, name: str, fields: Dict[str, str]) -> Type[SQLiteModel]:
        model_class = type(name, (SQLiteModel,), {
            '__fields__': fields,
            '__tablename__': name
        })
        self.cursor.execute('INSERT OR REPLACE INTO _models (name, fields) VALUES (?, ?)',
                            (name, json.dumps(fields)))
        self.connection.commit()
        
        create_table_sql = model_class.create_table()
        self.cursor.execute(create_table_sql)
        self.connection.commit()
        
        self.models[name] = model_class
        return model_class
    
    def get_models(self) -> List[str]:
        self.cursor.execute("SELECT name FROM _models")
        return [row[0] for row in self.cursor.fetchall()]

    def store_data(self, model_name: str, data: Dict[str, Any]) -> Any:
        model_class = self._get_model_class(model_name)
        instance = model_class(**data)
        insert_sql, params = instance.get_insert_sql()
        self.cursor.execute(insert_sql, params)
        self.connection.commit()
        return self.cursor.lastrowid

    def retrieve_data(self, model_name: str, filters: Dict[str, Any] = None) -> List[SQLiteModel]:
        model_class = self._get_model_class(model_name)
        select_sql, params = model_class.get_select_sql(filters)
        self.cursor.execute(select_sql, params)
        rows = self.cursor.fetchall()
        return [model_class.from_db_row(row) for row in rows]

    def _get_model_class(self, model_name: str) -> Type[SQLiteModel]:
        if model_name in self.models:
            return self.models[model_name]
        
        self.cursor.execute('SELECT fields FROM _models WHERE name = ?', (model_name,))
        row = self.cursor.fetchone()
        if row is None:
            raise ValueError(f"Model {model_name} does not exist")
        fields = json.loads(row[0])
        model_class = type(model_name, (SQLiteModel,), {
            '__fields__': fields,
            '__tablename__': model_name
        })
        self.models[model_name] = model_class
        return model_class

    def close(self):
        if self.connection:
            self.connection.close()
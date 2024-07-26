from typing import Dict, Any, List, Tuple

class SQLiteModel:
    __fields__: Dict[str, str] = {}
    __tablename__: str = ''

    def __init__(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)

    @classmethod
    def create_table(cls) -> str:
        fields = []
        for name, field_type in cls.__fields__.items():
            if name == 'id' and field_type == 'Integer':
                fields.append(f"{name} INTEGER PRIMARY KEY AUTOINCREMENT")
            elif field_type == 'Integer':
                fields.append(f"{name} INTEGER")
            elif field_type == 'Float':
                fields.append(f"{name} REAL")
            elif field_type == 'String':
                fields.append(f"{name} TEXT")
            elif field_type == 'Boolean':
                fields.append(f"{name} INTEGER")
            else:
                fields.append(f"{name} TEXT")
        
        fields_str = ', '.join(fields)
        return f"CREATE TABLE IF NOT EXISTS {cls.__tablename__} ({fields_str})"

    def to_dict(self) -> Dict[str, Any]:
        return {field: getattr(self, field) for field in self.__fields__}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SQLiteModel':
        return cls(**data)

    def get_insert_sql(self) -> Tuple[str, Tuple]:
        fields = [field for field in self.__fields__ if field != 'id']
        placeholders = ', '.join(['?' for _ in fields])
        sql = f"INSERT INTO {self.__tablename__} ({', '.join(fields)}) VALUES ({placeholders})"
        values = tuple(getattr(self, field) for field in fields)
        return sql, values

    @classmethod
    def get_select_sql(cls, filters: Dict[str, Any] = None) -> Tuple[str, Tuple]:
        sql = f"SELECT * FROM {cls.__tablename__}"
        params = []
        if filters:
            where_clauses = []
            for key, value in filters.items():
                where_clauses.append(f"{key} = ?")
                params.append(value)
            sql += " WHERE " + " AND ".join(where_clauses)
        return sql, tuple(params)

    @classmethod
    def from_db_row(cls, row: Tuple) -> 'SQLiteModel':
        return cls(**dict(zip(cls.__fields__.keys(), row)))
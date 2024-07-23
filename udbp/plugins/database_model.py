class DBfield:
    def __init__(self, db_type, autoincrement=False, foreign_key=None,
                 primary_key=False, unique=False, nullable=False) -> None:
        self.db_type = db_type
        self.primary_key = primary_key
        self.autoincrement = autoincrement
        self.foreign_key = foreign_key
        self.unique = unique
        self.nullable = nullable

    def sanitize_data(self, data):
        pass


class DBint(DBfield):
    def __init__(self, **kwargs) -> None:
        super().__init__('INTEGER', **kwargs)


class DBtext(DBfield):
    def __init__(self, **kwargs) -> None:
        super().__init__('TEXT', **kwargs)


class DBreal(DBfield):
    def __init__(self, **kwargs) -> None:
        super().__init__('REAL', **kwargs)


class DBblob(DBfield):
    def __init__(self, **kwargs) -> None:
        super().__init__('BLOB', **kwargs)


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        fields = {key: value for key, value in attrs.items() if isinstance(value, DBfield)}
        attrs['_fields'] = fields
        return super().__new__(cls, name, bases, attrs)


class Model(metaclass=ModelMeta):
    def __init__(self, **kwargs) -> None:
        for key, value in self.sanitize_data(kwargs).items():
            setattr(self, key, value)

    def to_dict(self):
        return {key: getattr(self, key) for key in self._fields.keys()}

    def sanitize_data(self, data: dict):
        for key, value in data.items():
            if key not in self._fields:
                raise ValueError(f'Cannot sanitize, Invalid field: {key}')
            expected_type = self._fields[key].db_type
            if not isinstance(value, self.sql_type_to_python(expected_type)):
                try:
                    data[key] = self.try_convert(value, expected_type)
                except ValueError:
                    raise ValueError(f'Invalid data type for field {key}: expected {expected_type}, got {type(value)}')
        return data

    @classmethod
    def try_convert(cls, data, sql_type):
        try:
            if isinstance(data, list):
                data = ', '.join(map(str, data))

            if data is None or data == '':
                if sql_type in ['INTEGER', 'REAL']:
                    return 0
                return ''

            if sql_type == 'INTEGER':
                return int(float(data))
            elif sql_type == 'TEXT':
                return str(data)
            elif sql_type == 'REAL':
                if isinstance(data, str):
                    data = data.replace(',', '.')

                    # Handle fractions within ranges (e.g., 1/2-2)
                    if '-' in data and '/' in data:
                        range_parts = data.split('-')
                        start = range_parts[0]
                        end = range_parts[1]

                        # Convert start part (which may be a fraction)
                        if '/' in start:
                            num, denom = start.split('/')
                            start = float(num) / float(denom)
                        else:
                            start = float(start)

                        end = float(end)
                        return (start + end) / 2

                    # Handle fractions (e.g., 1/2)
                    if '/' in data:
                        num, denom = data.split('/')
                        return float(num) / float(denom)

                    # Handle ranges (e.g., 1-2)
                    if '-' in data:
                        start, end = data.split('-')
                        return (float(start) + float(end)) / 2

                    # Handle special fraction character ½
                    if '½' in data:
                        data = data.replace('½', '.5')

                return float(data)
            
            raise ValueError(f'Unsupported SQL type: {sql_type}')
        except (ValueError, TypeError) as e:
            raise ValueError(f'Invalid data type for field: expected {sql_type}, got {type(data).__name__} - {e}')

    def sql_type_to_python(self, sql_type):
        if sql_type == 'INTEGER':
            return int
        elif sql_type == 'TEXT':
            return str
        elif sql_type == 'REAL':
            return float
        elif sql_type == 'BLOB':
            return bytearray
        else:
            raise ValueError(f'Invalid SQL type: {sql_type}')

    @classmethod
    def create_table(cls, db_type='sqlite3') -> str:
        if db_type == 'sqlite3':
            return cls.create_table_sqlite3()
        else:
            raise ValueError(f'Invalid database type: {db_type}')

    def create_insert(self, db_type='sqlite3') -> tuple[str, dict]:
        if db_type == 'sqlite3':
            return self.create_sqlite3_insert(), self.to_dict()
        else:
            raise ValueError(f'Invalid database type: {db_type}')

    @classmethod
    def create_sqlite3_insert(cls) -> tuple[str, dict]:
        # fields = cls._fields.keys()
        fields = [field for field, value in cls._fields.items() if not value.autoincrement]
        fields_str = ', '.join([f'"{field}"' for field in fields])
        values_str = ', '.join([f':{field}' for field in fields])
        return f'INSERT INTO "{cls.__name__}" ({fields_str}) VALUES ({values_str})'

    @classmethod
    def create_table_sqlite3(cls) -> str:
        fields_definition = []
        foreign_keys = []
        for name, field in cls._fields.items():
            field_def = f'"{name}" {field.db_type}'
            if field.primary_key:
                field_def += " PRIMARY KEY"
                if field.autoincrement:
                    field_def += " AUTOINCREMENT"
            if field.unique:
                field_def += " UNIQUE"
            if not field.nullable:
                field_def += " NOT NULL"
            if field.foreign_key:
                ref_table, ref_field = field.foreign_key
                foreign_keys.append(f'FOREIGN KEY("{name}") REFERENCES "{ref_table}"("{ref_field}") ON DELETE CASCADE')
            fields_definition.append(field_def)
        fields_sql = ', '.join(fields_definition + foreign_keys)
        sql = f'CREATE TABLE IF NOT EXISTS "{cls.__name__}" ({fields_sql})'
        return sql

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join([f'{key}={value}' for key, value in self.__dict__.items()])})"

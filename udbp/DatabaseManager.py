import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
import threading
from typing import Any, Dict, Type
from queue import Queue

from udbp.Handlers.SQLiteHandler import SQLiteHandler
from udbp.Handlers.BaseHandler import BaseHandler


class ConnectionPool:
    def __init__(self, db_type: str, config: Dict[str, Any], max_connections: int = 5):
        self.db_type = db_type
        self.config = config
        self.pool = Queue(max_connections)
        self.lock = threading.Lock()

        for _ in range(max_connections):
            connection = self._create_connection()
            self.pool.put(connection)

    def _create_connection(self):
        if self.db_type == 'sqlite':
            handler = SQLiteHandler(self.config['db_name'], self.config)
            handler.initialize()
            return handler
        # Add other database types here

    def get_connection(self):
        return self.pool.get()

    def release_connection(self, connection):
        self.pool.put(connection)


class DatabaseManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.handlers: Dict[str, BaseHandler] = {}
        self.executor = ThreadPoolExecutor(max_workers=config.get('max_workers', 5))
        self.logger = logging.getLogger(__name__)

    def get_handler(self, db_name: str, db_type: str) -> BaseHandler:
        if db_name not in self.handlers:
            handler_class = self._get_handler_class(db_type)
            handler = handler_class(db_name, self.config.get(db_type, {}))
            handler.initialize()
            self.handlers[db_name] = handler
        return self.handlers[db_name]

    def _get_handler_class(self, db_type: str) -> Type[BaseHandler]:
        if db_type == 'sqlite':
            return SQLiteHandler
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    async def execute_operation(self, db_name: str, db_type: str, operation: str, **kwargs) -> Any:
        handler = self.get_handler(db_name, db_type)
        try:
            method = getattr(handler, operation)
            loop = asyncio.get_event_loop()
            partial_method = partial(method, **kwargs)
            result = await loop.run_in_executor(self.executor, partial_method)
            self.logger.info(f"Operation {operation} completed successfully on {db_name}")
            return result
        except Exception as e:
            self.logger.error(f"Error executing {operation} on {db_name}: {str(e)}")
            raise

    async def create_model(self, db_name: str, db_type: str, model_name: str, fields: Dict[str, str]):
        return await self.execute_operation(db_name, db_type, 'create_model', name=model_name, fields=fields)

    async def store_data(self, db_name: str, db_type: str, model_name: str, data: Dict[str, Any]):
        return await self.execute_operation(db_name, db_type, 'store_data', model_name=model_name, data=data)

    async def retrieve_data(self, db_name: str, db_type: str, model_name: str, filters: Dict[str, Any] = None):
        return await self.execute_operation(db_name, db_type, 'retrieve_data', model_name=model_name, filters=filters)

    def shutdown(self):
        self.executor.shutdown(wait=True)
        for handler in self.handlers.values():
            handler.close()


from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

from udbp.Models.BaseModel import BaseModel


class BaseHandler(ABC):
    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def create_model(self, name: str, fields: Dict[str, str]) -> Type[BaseModel]:
        pass

    @abstractmethod
    def store_data(self, model_name: str, data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def retrieve_data(self, model_name: str, filters: Dict[str, Any] = None) -> List[BaseModel]:
        pass

    @abstractmethod
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        pass

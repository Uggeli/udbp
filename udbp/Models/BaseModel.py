"""Abstract base class for models"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type


class BaseModel(ABC):
    @classmethod
    @abstractmethod
    def create_table(cls) -> str:
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        pass

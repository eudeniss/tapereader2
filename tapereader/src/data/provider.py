"""
Classe base para providers de dados
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataProvider(ABC):
    """Classe base abstrata para providers de dados"""
    
    def __init__(self, config: Dict[str, Any], cache_manager: Any):
        self.config = config
        self.cache_manager = cache_manager
        self.is_connected = False
        
    @abstractmethod
    async def initialize(self):
        """Inicializa o provider"""
        pass
    
    @abstractmethod
    async def get_data(self) -> Optional[Dict[str, Any]]:
        """Obtém dados do provider"""
        pass
    
    @abstractmethod
    async def close(self):
        """Fecha conexão do provider"""
        pass
    
    async def health_check(self) -> bool:
        """Verifica se o provider está funcionando"""
        return self.is_connected
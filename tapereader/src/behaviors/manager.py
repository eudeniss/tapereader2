"""
Gerenciador de comportamentos do Tape Reader
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from .absorption import AbsorptionDetector
from .exhaustion import ExhaustionDetector
from ..core.models import Symbol, Behavior

logger = logging.getLogger(__name__)

class BehaviorManager:
    """Gerencia detecção de comportamentos de mercado"""
    
    def __init__(self, config: Dict[str, Any], cache_manager: Any):
        self.config = config
        self.cache_manager = cache_manager
        self.active_behaviors = []
        
        # Inicializa detectores
        behaviors_config = config.get('behaviors', {})
        
        # Detector de absorção
        if behaviors_config.get('absorption', {}).get('enabled', True):
            self.absorption_detector = AbsorptionDetector(
                behaviors_config.get('absorption', {})
            )
        else:
            self.absorption_detector = None
            
        # Detector de exaustão
        if behaviors_config.get('exhaustion', {}).get('enabled', True):
            self.exhaustion_detector = ExhaustionDetector(
                behaviors_config.get('exhaustion', {})
            )
        else:
            self.exhaustion_detector = None
            
        logger.info("BehaviorManager inicializado")
        
    async def detect(self, data: Dict[str, Any]) -> List[Behavior]:
        """Detecta comportamentos nos dados"""
        behaviors_detected = []
        
        if not data or not data.get('trades'):
            return behaviors_detected
            
        trades = data['trades']
        
        # Separa por símbolo
        wdo_trades = [t for t in trades if t.get('symbol') == 'WDOFUT']
        dol_trades = [t for t in trades if t.get('symbol') == 'DOLFUT']
        
        # Detecta comportamentos para WDOFUT
        if wdo_trades:
            behaviors_detected.extend(
                await self._detect_for_symbol(wdo_trades, Symbol.WDOFUT)
            )
            
        # Detecta comportamentos para DOLFUT
        if dol_trades:
            behaviors_detected.extend(
                await self._detect_for_symbol(dol_trades, Symbol.DOLFUT)
            )
            
        # Atualiza lista de comportamentos ativos
        self._update_active_behaviors(behaviors_detected)
        
        return behaviors_detected
        
    async def _detect_for_symbol(self, trades: List[Dict], symbol: Symbol) -> List[Behavior]:
        """Detecta comportamentos para um símbolo específico"""
        behaviors = []
        
        # Detecta absorção
        if self.absorption_detector and len(trades) >= 10:
            absorption = await self.absorption_detector.detect(trades, symbol)
            if absorption:
                behaviors.append(absorption)
                logger.info(f"Absorção detectada em {symbol}: confiança {absorption.confidence:.2f}")
                
        # Detecta exaustão
        if self.exhaustion_detector and len(trades) >= 30:
            exhaustion = await self.exhaustion_detector.detect(trades, symbol)
            if exhaustion:
                behaviors.append(exhaustion)
                logger.info(f"Exaustão detectada em {symbol}: confiança {exhaustion.confidence:.2f}")
                
        return behaviors
        
    def _update_active_behaviors(self, new_behaviors: List[Behavior]):
        """Atualiza lista de comportamentos ativos"""
        # Remove comportamentos antigos (mais de 5 minutos)
        now = datetime.now()
        self.active_behaviors = [
            b for b in self.active_behaviors
            if (now - b.timestamp).seconds < 300
        ]
        
        # Adiciona novos comportamentos
        self.active_behaviors.extend(new_behaviors)
        
        # Mantém apenas os últimos 50
        if len(self.active_behaviors) > 50:
            self.active_behaviors = self.active_behaviors[-50:]
            
    async def get_active_behaviors(self) -> List[Behavior]:
        """Retorna comportamentos ativos"""
        return self.active_behaviors
        
    def update_config(self, config: Dict[str, Any]):
        """Atualiza configuração"""
        self.config = config
        
        # Atualiza detectores
        behaviors_config = config.get('behaviors', {})
        
        if self.absorption_detector:
            self.absorption_detector.config = behaviors_config.get('absorption', {})
            
        if self.exhaustion_detector:
            self.exhaustion_detector.config = behaviors_config.get('exhaustion', {})
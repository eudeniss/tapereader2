"""
Detector de absorção - identifica quando grandes volumes são absorvidos sem movimento de preço
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from src.core.models import Behavior, BehaviorType, Symbol

logger = logging.getLogger(__name__)

class AbsorptionDetector:
    """Detecta comportamento de absorção"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.volume_threshold = config.get('volume_threshold', 1000)
        self.price_impact_max = config.get('price_impact_max', 0.0001)
        self.time_window = config.get('time_window', 60)
        
    async def detect(self, trades: List[Dict[str, Any]], symbol: Symbol) -> Optional[Behavior]:
        """Detecta absorção nos trades"""
        if len(trades) < 10:
            return None
            
        # Agrupa trades por janela de tempo
        now = datetime.now()
        recent_trades = [
            t for t in trades 
            if isinstance(t.get('timestamp'), datetime) and 
            (now - t['timestamp']).seconds < self.time_window
        ]
        
        if not recent_trades:
            return None
            
        # Calcula volume total
        total_volume = sum(t.get('volume', 0) for t in recent_trades)
        
        if total_volume < self.volume_threshold:
            return None
            
        # Calcula variação de preço
        prices = [t.get('price', 0) for t in recent_trades if t.get('price')]
        if not prices:
            return None
            
        price_range = max(prices) - min(prices)
        avg_price = sum(prices) / len(prices)
        price_impact = price_range / avg_price if avg_price > 0 else 0
        
        # Detecta absorção: alto volume com baixo impacto no preço
        if price_impact < self.price_impact_max:
            confidence = min(1.0, total_volume / (self.volume_threshold * 2))
            
            return Behavior(
                type=BehaviorType.ABSORPTION,
                symbol=symbol,
                timestamp=now,
                confidence=confidence,
                metadata={
                    'volume': total_volume,
                    'price_impact': price_impact,
                    'trade_count': len(recent_trades),
                    'time_window': self.time_window
                }
            )
            
        return None
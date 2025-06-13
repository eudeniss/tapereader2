"""
Detector de exaustão - identifica quando um movimento perde força
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.core.models import Behavior, BehaviorType, Symbol

logger = logging.getLogger(__name__)

class ExhaustionDetector:
    """Detecta comportamento de exaustão"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.momentum_decay_rate = config.get('momentum_decay_rate', 0.7)
        self.volume_decay_rate = config.get('volume_decay_rate', 0.6)
        self.confirmation_bars = config.get('confirmation_bars', 3)
        self.history = []
        
    async def detect(self, trades: List[Dict[str, Any]], symbol: Symbol) -> Optional[Behavior]:
        """Detecta exaustão nos trades"""
        if len(trades) < self.confirmation_bars * 10:
            return None
            
        # Divide trades em barras (grupos)
        bars = self._create_bars(trades, self.confirmation_bars + 2)
        
        if len(bars) < self.confirmation_bars:
            return None
            
        # Analisa momentum e volume
        momentum_declining = self._check_momentum_decline(bars)
        volume_declining = self._check_volume_decline(bars)
        
        if momentum_declining and volume_declining:
            confidence = (momentum_declining + volume_declining) / 2
            
            return Behavior(
                type=BehaviorType.EXHAUSTION,
                symbol=symbol,
                timestamp=datetime.now(),
                confidence=confidence,
                metadata={
                    'momentum_score': momentum_declining,
                    'volume_score': volume_declining,
                    'bars_analyzed': len(bars),
                    'direction': self._get_direction(bars)
                }
            )
            
        return None
        
    def _create_bars(self, trades: List[Dict[str, Any]], num_bars: int) -> List[Dict[str, Any]]:
        """Cria barras a partir dos trades"""
        if not trades:
            return []
            
        trades_per_bar = max(1, len(trades) // num_bars)
        bars = []
        
        for i in range(0, len(trades), trades_per_bar):
            bar_trades = trades[i:i + trades_per_bar]
            if bar_trades:
                bars.append({
                    'open': bar_trades[0].get('price', 0),
                    'close': bar_trades[-1].get('price', 0),
                    'high': max(t.get('price', 0) for t in bar_trades),
                    'low': min(t.get('price', 0) for t in bar_trades),
                    'volume': sum(t.get('volume', 0) for t in bar_trades),
                    'trades': len(bar_trades)
                })
                
        return bars
        
    def _check_momentum_decline(self, bars: List[Dict[str, Any]]) -> float:
        """Verifica declínio no momentum"""
        if len(bars) < 2:
            return 0.0
            
        # Calcula variação de preço por barra
        price_changes = []
        for i in range(1, len(bars)):
            change = abs(bars[i]['close'] - bars[i-1]['close'])
            price_changes.append(change)
            
        # Verifica se está diminuindo
        declining = 0
        for i in range(1, len(price_changes)):
            if price_changes[i] < price_changes[i-1] * self.momentum_decay_rate:
                declining += 1
                
        return declining / (len(price_changes) - 1) if len(price_changes) > 1 else 0
        
    def _check_volume_decline(self, bars: List[Dict[str, Any]]) -> float:
        """Verifica declínio no volume"""
        if len(bars) < 2:
            return 0.0
            
        volumes = [bar['volume'] for bar in bars]
        
        # Verifica se está diminuindo
        declining = 0
        for i in range(1, len(volumes)):
            if volumes[i] < volumes[i-1] * self.volume_decay_rate:
                declining += 1
                
        return declining / (len(volumes) - 1) if len(volumes) > 1 else 0
        
    def _get_direction(self, bars: List[Dict[str, Any]]) -> str:
        """Determina direção do movimento"""
        if not bars:
            return 'NEUTRAL'
            
        first_close = bars[0]['close']
        last_close = bars[-1]['close']
        
        if last_close > first_close * 1.001:
            return 'BULLISH'
        elif last_close < first_close * 0.999:
            return 'BEARISH'
        else:
            return 'NEUTRAL'
"""
Analisadores de mercado para o Tape Reader
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class VolumeAnalyzer:
    """Analisador de volume"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.volume_history = []
        
    async def analyze(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa padrões de volume"""
        if not trades:
            return {}
            
        total_volume = sum(trade.get('volume', 0) for trade in trades)
        avg_volume = total_volume / len(trades) if trades else 0
        
        # Adiciona ao histórico
        self.volume_history.append({
            'timestamp': datetime.now(),
            'total': total_volume,
            'average': avg_volume,
            'count': len(trades)
        })
        
        # Mantém apenas últimos 100 registros
        if len(self.volume_history) > 100:
            self.volume_history = self.volume_history[-100:]
        
        return {
            'current_volume': total_volume,
            'average_volume': avg_volume,
            'trade_count': len(trades),
            'volume_trend': self._calculate_trend()
        }
        
    def _calculate_trend(self) -> str:
        """Calcula tendência do volume"""
        if len(self.volume_history) < 10:
            return 'NEUTRAL'
            
        recent = sum(h['total'] for h in self.volume_history[-5:])
        older = sum(h['total'] for h in self.volume_history[-10:-5])
        
        if recent > older * 1.2:
            return 'INCREASING'
        elif recent < older * 0.8:
            return 'DECREASING'
        else:
            return 'NEUTRAL'


class PriceActionAnalyzer:
    """Analisador de ação do preço"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.price_history = []
        
    async def analyze(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa padrões de preço"""
        if not trades:
            return {}
            
        prices = [trade.get('price', 0) for trade in trades]
        if not prices:
            return {}
            
        current_price = prices[-1]
        high_price = max(prices)
        low_price = min(prices)
        avg_price = sum(prices) / len(prices)
        
        # Adiciona ao histórico
        self.price_history.append({
            'timestamp': datetime.now(),
            'price': current_price,
            'high': high_price,
            'low': low_price,
            'average': avg_price
        })
        
        # Mantém apenas últimos 100 registros
        if len(self.price_history) > 100:
            self.price_history = self.price_history[-100:]
        
        return {
            'current_price': current_price,
            'high': high_price,
            'low': low_price,
            'average': avg_price,
            'range': high_price - low_price,
            'price_trend': self._calculate_trend(),
            'volatility': self._calculate_volatility()
        }
        
    def _calculate_trend(self) -> str:
        """Calcula tendência do preço"""
        if len(self.price_history) < 10:
            return 'NEUTRAL'
            
        recent_avg = sum(h['price'] for h in self.price_history[-5:]) / 5
        older_avg = sum(h['price'] for h in self.price_history[-10:-5]) / 5
        
        diff_percent = (recent_avg - older_avg) / older_avg * 100
        
        if diff_percent > 0.1:
            return 'BULLISH'
        elif diff_percent < -0.1:
            return 'BEARISH'
        else:
            return 'NEUTRAL'
            
    def _calculate_volatility(self) -> float:
        """Calcula volatilidade"""
        if len(self.price_history) < 5:
            return 0.0
            
        recent_prices = [h['price'] for h in self.price_history[-5:]]
        avg = sum(recent_prices) / len(recent_prices)
        variance = sum((p - avg) ** 2 for p in recent_prices) / len(recent_prices)
        return (variance ** 0.5) / avg * 100  # Volatilidade em %


class OrderFlowAnalyzer:
    """Analisador de fluxo de ordens"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.flow_history = []
        
    async def analyze(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa fluxo de ordens"""
        if not trades:
            return {}
            
        buy_volume = sum(t.get('volume', 0) for t in trades if t.get('side') == 'BUY')
        sell_volume = sum(t.get('volume', 0) for t in trades if t.get('side') == 'SELL')
        total_volume = buy_volume + sell_volume
        
        if total_volume == 0:
            return {}
            
        buy_ratio = buy_volume / total_volume
        sell_ratio = sell_volume / total_volume
        
        # Delta de volume
        volume_delta = buy_volume - sell_volume
        delta_percent = volume_delta / total_volume * 100 if total_volume > 0 else 0
        
        # Adiciona ao histórico
        self.flow_history.append({
            'timestamp': datetime.now(),
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'delta': volume_delta,
            'delta_percent': delta_percent
        })
        
        # Mantém apenas últimos 100 registros
        if len(self.flow_history) > 100:
            self.flow_history = self.flow_history[-100:]
        
        return {
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'total_volume': total_volume,
            'buy_ratio': buy_ratio,
            'sell_ratio': sell_ratio,
            'volume_delta': volume_delta,
            'delta_percent': delta_percent,
            'flow_bias': self._calculate_bias(),
            'aggression': self._calculate_aggression(trades)
        }
        
    def _calculate_bias(self) -> str:
        """Calcula viés do fluxo"""
        if len(self.flow_history) < 5:
            return 'NEUTRAL'
            
        recent_delta = sum(h['delta'] for h in self.flow_history[-5:])
        
        if recent_delta > 0:
            return 'BULLISH'
        elif recent_delta < 0:
            return 'BEARISH'
        else:
            return 'NEUTRAL'
            
    def _calculate_aggression(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calcula agressão de compradores vs vendedores"""
        aggressive_buys = sum(1 for t in trades 
                            if t.get('side') == 'BUY' and t.get('aggressor', False))
        aggressive_sells = sum(1 for t in trades 
                             if t.get('side') == 'SELL' and t.get('aggressor', False))
        
        total_trades = len(trades)
        
        return {
            'buy_aggression': aggressive_buys / total_trades if total_trades > 0 else 0,
            'sell_aggression': aggressive_sells / total_trades if total_trades > 0 else 0
        }


class ImbalanceAnalyzer:
    """Analisador de desequilíbrios"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.imbalance_threshold = config.get('threshold', 0.7)  # 70% de desequilíbrio
        
    async def analyze(self, book: Dict[str, Any], trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa desequilíbrios no book e fluxo"""
        result = {
            'book_imbalance': self._analyze_book_imbalance(book),
            'flow_imbalance': self._analyze_flow_imbalance(trades),
            'detected_imbalances': []
        }
        
        # Detecta desequilíbrios significativos
        if result['book_imbalance'].get('ratio', 0) > self.imbalance_threshold:
            result['detected_imbalances'].append({
                'type': 'BOOK',
                'direction': result['book_imbalance']['direction'],
                'strength': result['book_imbalance']['ratio']
            })
            
        if abs(result['flow_imbalance'].get('ratio', 0)) > self.imbalance_threshold:
            direction = 'BUY' if result['flow_imbalance']['ratio'] > 0 else 'SELL'
            result['detected_imbalances'].append({
                'type': 'FLOW',
                'direction': direction,
                'strength': abs(result['flow_imbalance']['ratio'])
            })
        
        return result
        
    def _analyze_book_imbalance(self, book: Dict[str, Any]) -> Dict[str, Any]:
        """Analisa desequilíbrio no book de ofertas"""
        if not book:
            return {}
            
        bids = book.get('bids', [])
        asks = book.get('asks', [])
        
        if not bids or not asks:
            return {}
            
        # Volume total de cada lado
        bid_volume = sum(b.get('volume', 0) for b in bids)
        ask_volume = sum(a.get('volume', 0) for a in asks)
        total_volume = bid_volume + ask_volume
        
        if total_volume == 0:
            return {}
            
        # Calcula ratio
        bid_ratio = bid_volume / total_volume
        ask_ratio = ask_volume / total_volume
        
        # Determina direção do desequilíbrio
        if bid_ratio > ask_ratio:
            direction = 'BID_HEAVY'
            ratio = bid_ratio
        else:
            direction = 'ASK_HEAVY'
            ratio = ask_ratio
            
        return {
            'bid_volume': bid_volume,
            'ask_volume': ask_volume,
            'bid_ratio': bid_ratio,
            'ask_ratio': ask_ratio,
            'direction': direction,
            'ratio': ratio,
            'is_imbalanced': ratio > self.imbalance_threshold
        }
        
    def _analyze_flow_imbalance(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa desequilíbrio no fluxo de trades"""
        if not trades:
            return {}
            
        # Agrupa por níveis de preço
        price_levels = {}
        
        for trade in trades:
            price = trade.get('price', 0)
            volume = trade.get('volume', 0)
            side = trade.get('side', '')
            
            if price not in price_levels:
                price_levels[price] = {'buy': 0, 'sell': 0}
                
            if side == 'BUY':
                price_levels[price]['buy'] += volume
            else:
                price_levels[price]['sell'] += volume
        
        # Calcula desequilíbrio por nível
        imbalanced_levels = []
        
        for price, volumes in price_levels.items():
            total = volumes['buy'] + volumes['sell']
            if total == 0:
                continue
                
            buy_ratio = volumes['buy'] / total
            sell_ratio = volumes['sell'] / total
            
            if max(buy_ratio, sell_ratio) > self.imbalance_threshold:
                imbalanced_levels.append({
                    'price': price,
                    'buy_ratio': buy_ratio,
                    'sell_ratio': sell_ratio,
                    'dominant_side': 'BUY' if buy_ratio > sell_ratio else 'SELL'
                })
        
        # Calcula desequilíbrio geral
        total_buy = sum(pl['buy'] for pl in price_levels.values())
        total_sell = sum(pl['sell'] for pl in price_levels.values())
        total = total_buy + total_sell
        
        if total == 0:
            return {}
            
        flow_ratio = (total_buy - total_sell) / total
        
        return {
            'total_buy': total_buy,
            'total_sell': total_sell,
            'ratio': flow_ratio,
            'imbalanced_levels': imbalanced_levels,
            'imbalanced_count': len(imbalanced_levels)
        }
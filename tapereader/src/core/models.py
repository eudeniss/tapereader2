"""
Modelos de dados do Tape Reader
"""
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List

class Symbol(str, Enum):
    DOLFUT = "DOLFUT"
    WDOFUT = "WDOFUT"

class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    
    @property
    def opposite(self):
        return Side.SELL if self == Side.BUY else Side.BUY

class BehaviorType(str, Enum):
    ABSORPTION = "absorption"
    EXHAUSTION = "exhaustion"
    INSTITUTIONAL = "institutional"
    STOP_HUNT = "stop_hunt"
    SWEEP = "sweep"

@dataclass
class Trade:
    timestamp: datetime
    symbol: Symbol
    price: float
    volume: int
    side: Side
    aggressor: bool
    trade_id: Optional[str] = None
    
    @property
    def value(self):
        return self.price * self.volume

@dataclass
class Behavior:
    type: BehaviorType
    symbol: Symbol
    timestamp: datetime
    confidence: float
    metadata: Dict[str, Any]
    
    def to_dict(self):
        return {
            'type': self.type.value,
            'symbol': self.symbol.value,
            'timestamp': self.timestamp.isoformat(),
            'confidence': self.confidence,
            'metadata': self.metadata
        }

@dataclass
class Signal:
    symbol: Symbol
    side: Side
    entry_price: float
    stop_loss: float
    take_profit: float
    confidence: float
    behaviors: List[Behavior]
    timestamp: datetime
    
    def to_dict(self):
        return {
            'symbol': self.symbol.value,
            'side': self.side.value,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'confidence': self.confidence,
            'behaviors': [b.type.value for b in self.behaviors],
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class Position:
    symbol: Symbol
    side: Side
    entry_price: float
    size: int
    stop_loss: float
    take_profit: float
    entry_time: datetime
    pnl: float = 0.0
    status: str = 'OPEN'
    
    def to_dict(self):
        return {
            'symbol': self.symbol.value,
            'side': self.side.value,
            'entry_price': self.entry_price,
            'size': self.size,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'entry_time': self.entry_time.isoformat(),
            'pnl': self.pnl,
            'status': self.status
        }

@dataclass
class SystemState:
    timestamp: datetime
    active_signals: List[Signal]
    daily_pnl: float
    positions: List[Position]
    cache_stats: Dict[str, Any]
    
    def to_dict(self):
        return {
            'timestamp': self.timestamp.isoformat(),
            'active_signals': [s.to_dict() for s in self.active_signals],
            'daily_pnl': self.daily_pnl,
            'positions': [p.to_dict() for p in self.positions],
            'cache_stats': self.cache_stats
        }
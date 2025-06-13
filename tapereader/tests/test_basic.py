import pytest
import asyncio
from datetime import datetime
import sys
import os

# Adiciona tapereader ao path
tapereader_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, tapereader_path)

# Agora importa usando caminhos absolutos
from src.core.models import Trade, Symbol, Side, Behavior, BehaviorType
from src.behaviors.absorption import AbsorptionDetector
from src.behaviors.exhaustion import ExhaustionDetector

class TestModels:
    """Testa modelos basicos"""
    
    def test_trade_creation(self):
        """Testa criacao de trade"""
        trade = Trade(
            timestamp=datetime.utcnow(),
            symbol=Symbol.DOLFUT,
            price=5000.0,
            volume=100,
            side=Side.BUY,
            aggressor=True
        )
        
        assert trade.symbol == Symbol.DOLFUT
        assert trade.price == 5000.0
        assert trade.volume == 100
        assert trade.value == 500000.0

    def test_side_opposite(self):
        """Testa lado oposto"""
        assert Side.BUY.opposite == Side.SELL
        assert Side.SELL.opposite == Side.BUY

class TestBehaviors:
    """Testa detectores de comportamento"""
    
    @pytest.mark.asyncio
    async def test_absorption_detector(self):
        """Testa detector de absorcao"""
        config = {
            'volume_threshold': 1000,
            'price_impact_max': 0.0001,
            'time_window': 60
        }
        
        detector = AbsorptionDetector(config)
        assert detector is not None
        
        # Testa com dados simples
        trades = [{
            'timestamp': datetime.utcnow(),
            'price': 5000.0,
            'volume': 1000,
            'side': 'BUY',
            'aggressor': True
        }]
        
        behavior = await detector.detect(trades, Symbol.DOLFUT)
        # Pode ser None com poucos dados
        assert behavior is None or behavior.type == BehaviorType.ABSORPTION

@pytest.fixture
def event_loop():
    """Event loop para testes async"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

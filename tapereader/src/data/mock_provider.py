import random
from datetime import datetime, timezone
from .provider import DataProvider

class MockDataProvider(DataProvider):
    async def initialize(self):
        self.price = 5000.0
        print("Mock provider initialized")
    
    async def get_data(self):
        self.price += random.uniform(-5, 5)
        return {
            'trades': [{
                'timestamp': datetime.now(timezone.utc),
                'symbol': 'DOLFUT',
                'price': self.price,
                'volume': random.randint(100, 1000),
                'side': 'BUY',
                'aggressor': True
            }],
            'book': {'bids': [], 'asks': []},
            'timestamp': datetime.now(timezone.utc)
        }
    
    async def close(self): pass

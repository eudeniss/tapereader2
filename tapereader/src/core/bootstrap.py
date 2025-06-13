import asyncio
from src.data.provider import DataProvider
from src.data.excel_provider import ExcelDataProvider
from src.data.mock_provider import MockDataProvider
from src.behaviors.manager import BehaviorManager
from src.strategies.decision_matrix import DecisionMatrix
from src.strategies.regime_classifier import RegimeClassifier
from src.strategies.confluence import ConfluenceAnalyzer
from src.strategies.risk_manager import RiskManager
from src.strategies.signal_tracker import SignalTracker
from src.cache.manager import CacheManager
from src.core.tracker import PerformanceTracker, StateManager

class Bootstrap:
    def __init__(self, config_manager, console):
        self.config_manager = config_manager
        self.config = config_manager.config
        self.console = console
        self.is_running = False
        self.tasks = []
        
    async def initialize(self):
        print("Initializing system...")
        self.cache_manager = CacheManager("./data/cache")
        await self.cache_manager.initialize()
        
        self.state_manager = StateManager("./data")
        await self.state_manager.initialize()
        
        self.performance_tracker = PerformanceTracker("./logs/analysis")
        
        if self.config.application.mode == 'test':
            self.data_provider = MockDataProvider({}, self.cache_manager)
        else:
            self.data_provider = ExcelDataProvider({}, self.cache_manager)
        await self.data_provider.initialize()
        
        self.behavior_manager = BehaviorManager({}, self.cache_manager)
        self.regime_classifier = RegimeClassifier(self.cache_manager)
        self.confluence_analyzer = ConfluenceAnalyzer(self.cache_manager)
        self.risk_manager = RiskManager({}, self.state_manager)
        self.signal_tracker = SignalTracker(self.risk_manager, self.performance_tracker)
        self.decision_matrix = DecisionMatrix({}, self.behavior_manager, self.regime_classifier, 
                                            self.confluence_analyzer, self.signal_tracker)
        
    async def run(self):
        self.is_running = True
        print("System running in", self.config.application.mode, "mode")
        print("Press Ctrl+C to stop")
        
        while self.is_running:
            try:
                data = await self.data_provider.get_data()
                if data and data.get('trades'):
                    print(f"Price: {data['trades'][0]['price']:.2f}")
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
                
    async def shutdown(self):
        self.is_running = False
        print("Shutting down...")

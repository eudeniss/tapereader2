class RegimeClassifier:
    def __init__(self, cache_manager):
        self.current_regime = {'type': 'RANGING', 'confidence': 0.5}
    async def classify(self): return self.current_regime
    async def get_current_regime(self): return self.current_regime

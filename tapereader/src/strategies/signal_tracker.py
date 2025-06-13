class SignalTracker:
    def __init__(self, risk_manager, performance_tracker):
        self.active_signals = []
    async def process_signal(self, signal): self.active_signals.append(signal)
    async def get_active_signals(self): return self.active_signals
    async def restore_signals(self, signals): self.active_signals = signals

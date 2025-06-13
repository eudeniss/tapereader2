class RiskManager:
    def __init__(self, config, state_manager):
        self.daily_pnl = 0.0
        self.positions = []
    async def get_daily_pnl(self): return self.daily_pnl
    async def get_positions(self): return self.positions
    async def get_status(self): return {'daily_loss': 0, 'max_daily_loss': 1000, 
        'open_positions': 0, 'max_positions': 3, 'total_exposure': 0, 
        'risk_level': 'LOW', 'can_trade': True}
    async def restore_pnl(self, pnl): self.daily_pnl = pnl
    def update_config(self, config): pass

class ConsoleDisplay:
    def __init__(self, config): self.config = config
    async def start(self): print("Console started")
    async def stop(self): pass
    async def update(self, stats): pass
    def info(self, text): print(f"INFO: {text}")
    def error(self, text): print(f"ERROR: {text}")
    def warning(self, text): print(f"WARNING: {text}")
    def success(self, text): print(f"SUCCESS: {text}")

class ErrorHandler:
    def __init__(self, console=None): self.console = console
    async def handle_error(self, error, context=None): 
        print(f"Error: {error}")

class GlobalErrorHandler:
    @classmethod
    def get_instance(cls, console=None): 
        return ErrorHandler(console)

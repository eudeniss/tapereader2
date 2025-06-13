import logging
def setup_logging(**kwargs):
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('tapereader')
def get_logger(name): return logging.getLogger(name)

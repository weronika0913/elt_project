import logging
import os 

def setup_logger(name):
    logger = logging.getLogger(name)
    logging.basicConfig(
        level=logging.INFO,
        filename=os.path.join('logs','app.log'),
        encoding='utf-8',
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt= '%Y-%m-%d %H:%M:%S'
                        )
    
    return logger



import logging

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def create_logger(logger_name:str,level=logging.INFO):
    custom_logger = logging.getLogger(logger_name)
    ch1 = logging.StreamHandler()
    ch1.setLevel(level)
    ch1.setFormatter(formatter)
    custom_logger.addHandler(ch1)
    custom_logger.setLevel(level)    
    return custom_logger
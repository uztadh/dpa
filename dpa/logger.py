import logging


def create_logger(name, minimal=True):
    logger = logging.getLogger(name)
    # set level
    logger.setLevel(logging.INFO)
    # add handler + formatting
    fmt, datefmt = "", ""
    if minimal:
        fmt = "%(asctime)s [%(levelname)s] - %(message)s"
        datefmt = "%H:%M:%S"
    else:
        fmt = "%(asctime)s %(process)d [%(levelname)s] (%(module)s) - %(message)s"
        datefmt = "%d-%b-%y %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    return logger


null = logging.getLogger("null")
null.addHandler(logging.NullHandler())
default = create_logger("dpa", minimal=True)

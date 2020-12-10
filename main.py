import logging
from concurrent import futures
from lib.BitMEX import Mexsocket
from lib.bitFlyer import Bfsocket

# 共通format
log_format = '%(asctime)s.%(msecs)-03d, %(levelname)-8s, [%(name)s: %(module)s.%(funcName)s %(lineno)d], %(message)s'
log_formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
# console出力Handler
logConsoleHandler = logging.StreamHandler()
logConsoleHandler.setFormatter(log_formatter)

# mex2
mex = Mexsocket(save=True)
mex.logger.addHandler(logConsoleHandler)
mex.logger.setLevel(logging.INFO)

# BF
bf = Bfsocket(save=True)
bf.logger.addHandler(logConsoleHandler)
bf.logger.setLevel(logging.DEBUG)


future_list = []
with futures.ThreadPoolExecutor(max_workers=2) as executor:
    # future_list.append(executor.submit(fn=bf.run))
    # future_list.append(executor.submit(fn=bf.manage_data))
    future_list.append(executor.submit(fn=mex.run))
    future_list.append(executor.submit(fn=mex.manage_data))
    _ = futures.as_completed(fs=future_list)
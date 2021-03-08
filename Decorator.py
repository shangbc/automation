# Time     2020/12/03 14::50
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import time
import logging
import re
from LoggingClass import Logging


# 记录当前时间
def timeLog(logName):
    def log(func):
        def warp(*args, **kwargs):
            startTime = time.time()
            fun = func(*args, **kwargs)
            enfTime = time.time()
            exeTime = enfTime - startTime
            strLog = logName + "执行时间：" + str(exeTime)[0:5] + "s"
            logging.info(strLog)
            return fun

        return warp

    return log

    # @Time     2020/09/15 11:33:00
    # Auhtor    shangxb
    # Verion    1.1.2
    # function
    #


def strFormat(string, dictVaule=None):
    string = string.expandtabs(tabsize=8)
    string = re.sub(r'[\s]+', ' ', string)
    # string = re.sub(r'\n ', '\n', string)
    string = string.strip()
    string = string + "\n"
    if dictVaule is not None:
        for key, vuale in dictVaule.items():
            strRe = "[:" + key + "]"
            string = string.replace(strRe, vuale)
    return string

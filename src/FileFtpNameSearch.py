# -*- coding:UTF-8 -*-
# Time     2020/12/08 16::21
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
import logging
import datetime
import sys
import os
from ftplib import FTP


# 设置日志
def logSetup():
    logPath = "E:/程序/pyhton/shangxb/src/log/"
    logFileName = logPath + "-" .join(["FileFtpNameSearch", str(datetime .datetime .now() .strftime("%Y%m%d"))]) + ".log"

    logger = logging .getLogger()
    logging.FileHandler()
    logging .StreamHandler()


if __name__ == '__main__':
    logSetup()
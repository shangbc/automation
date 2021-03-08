# -*- coding:UTF-8 -*-
# Time     2020/12/08 16::21
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
import time
import datetime
import logging
import sys
import os
import shutil
import re

# 获取1天前的当前时间
nowDate = datetime.datetime.now() - datetime.timedelta(days=0)
nowDateStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
nowDateLoggingStr = datetime.datetime.now().strftime("%Y%m%d")
nowDatefileStr = datetime.datetime.now().strftime("%Y%m%d%H")
# # 当前路径
# locatPath = sys.argv[1]
# # 远程路径
# SearcPath = sys.argv[2]
# # 保存的文件名
# fileName = sys.argv[3]
# # 远程ip
# ftpIp = sys.argv[4]
# # 用户
# ftpUser = sys.argv[5]
# # 密码
# ftpPassword = sys.argv[6]
# # 端口
# ftpPort = int(sys.argv[7])

upPath = "/app/khbz/ftpMonitor/"
SearcPath = "/app/cbapp/ftp_monitor/file"
logPath = "/app/cbapp/ftp_monitor/log/"
fileName = "FtpDirFileUp_"
ftpIp = "20.26.28.234"
ftpUser = "khbz"
ftpPassword = "khbz"
ftpPort = 21


# 连接ftp
def ftpconnect(host, port, username, password):
    ftp = FTP()
    # 打开调试级别2，显示详细信息
    # ftp.set_debuglevel(2)
    ftp.connect(host, port)
    ftp.encoding = 'utf-8'
    ftp.set_pasv(False)
    ftp.login(username, password)
    return ftp


def SearchDir(path='/'):
    try:
        fileList = []
        dirPath = None
        for nowPath, dirs, files in os.walk(path, topdown=True):
            dirPath = nowPath
            fileList = files
            break
    except Exception as error:
        logging.error(error)
    else:
        return dirPath, fileList
    finally:
        logging.info("目录：" + path + "检索完成！")


def resetLoggin():
    logPathFile = logPath + "" + fileName + nowDateLoggingStr + ".log"
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename=logPathFile,
                        # a是追加模式，默认如果不写的话，就是追加模式
                        # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        filemode='a',
                        # 日志格式
                        format=
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
                        )


def fileUp(path, fileList):
    for file in fileList:
        try:
            locaFile = path + "/" + file
            locaFileHis = path + "/his/" + file
            myFile = open(locaFile, 'rb')
            fileCodeDone = "STOR " + upPath + file
            ftp.storbinary(fileCodeDone, myFile)
        except Exception as error:
            myFile.flush()
            myFile.close()
            logging.error("文件上传失败：" + file)
            logging.error(error)
        else:
            myFile.flush()
            myFile.close()
            shutil.move(locaFile, locaFileHis)
            logging.info("文件上传成功：" + file)


if __name__ == '__main__':
    try:
        begin = time.time()
        dateTime_p = datetime.datetime.now()
        resetLoggin()
        ftp = ftpconnect(ftpIp, ftpPort, ftpUser, ftpPassword)
        locaPath, fileList = SearchDir(SearcPath)
        fileUp(locaPath, fileList)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("本次程序执行结束")
        sys.exit(0)

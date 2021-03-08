# -*- coding:UTF-8 -*-
# Time     2020/12/16 17::48
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认

import paramiko
from ftplib import FTP
import datetime
import time
import stat
import logging
import json
import os
import shutil

# 获取1天前的当前时间
nowDate = datetime.datetime.now() - datetime.timedelta(days=0)
nowDateStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
nowDateLoggingStr = datetime.datetime.now().strftime("%Y%m%d")
nowDatefileStr = datetime.datetime.now().strftime("%Y%m%d%H")
fileName = "SFTP_MONITOR_SFTP5711_"
ftpIp = "20.26.28.234"
ftpUser = "khbz"
ftpPassword = "khbz"
ftpPort = 21


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


def sftpConn(sftpIp, sftpUsername, sftpPassword, sftpPort):
    client = paramiko.SSHClient()  # 获取SSHClient实例
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(sftpIp, username=sftpUsername, password=sftpPassword, port=sftpPort)  # 连接SSH服务端
    # client.connect("192.168.65.128", username="silent", password="shangbc")  # 连接SSH服务端
    transport = client.get_transport()  # 获取Transport实例

    # 创建sftp对象，SFTPClient是定义怎么传输文件、怎么交互文件
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, client


def SearchDir(path):
    fileNum = 0
    dirList = sftp.listdir_attr(path)
    for dirFile in dirList:
        if stat.S_ISDIR(dirFile.st_mode):
            nowPath = path + ("/" if path != "/" else "") + dirFile.filename
            for bar in SearchDir(nowPath):
                yield bar
        else:
            fileTime = datetime.datetime.fromtimestamp(dirFile.st_mtime)
            if fileTime < nowDate:
                fileNum += 1
    yield fileNum, nowDateStr, path


def resetLoggin():
    locatPath = "E:/程序/pyhton/shangxb/src/file/"
    logPathFile = locatPath + fileName + nowDateLoggingStr + ".log"
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename=logPathFile,
                        # a是追加模式，默认如果不写的话，就是追加模式
                        # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        filemode='a',
                        # 日志格式
                        format=
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
                        )


@timeLog("程序")
def wirtFile(SearchDirFunc):
    try:
        locatPath = "E:/程序/pyhton/shangxb/src/file/"
        locatFileName = locatPath + fileName + nowDatefileStr + ".txt"
        locatFileNmaeHis = locatPath + "his/" + fileName + nowDatefileStr + ".txt"
        ftpFile = open(locatFileName, "w")
        dictList = []
        for dirs in SearchDirFunc:
            # pass
            num, myTime, path = dirs
            dicts = {
                "FTPIP": "192.168.65.128",
                "FTPUSER": "silent",
                "FTPPATH": path,
                "FILENUM": num,
                "EXECTIME": myTime
            }
            dictList.append(dicts)
            logging.info(path)
        jsonStr = json.dumps(dictList)
        ftpFile.write(jsonStr)
        ftpFile.flush()
        ftpFile.close()
    except Exception as error:
        logging.error(error)
    else:
        try:
            ftp = FTP()
            # 打开调试级别2，显示详细信息
            # ftp.set_debuglevel(2)
            ftp.connect(ftpIp, ftpPort)
            ftp.encoding = 'utf-8'
            ftp.set_pasv(False)
            ftp.login(ftpUser, ftpPassword)
            fileCodeUp = "STOR " + "/app/khbz/ftpMonitor/" + fileName + nowDatefileStr + ".txt"
            ftpFile = open(locatFileName, "rb")
            ftp.storbinary(fileCodeUp, ftpFile)
            ftpFile.flush()
            ftpFile.close()
            logging.info(fileName + nowDatefileStr + ".txt" + "监控文件上传20.26.28.234成功！")
            shutil.move(locatFileName, locatFileNmaeHis)
            # os.remove(locaFile)
        except Exception as error:
            logging.error(error)
            ftpFile.flush()
            ftpFile.close()
            os.remove(locatFileName)


if __name__ == '__main__':
    # ip = "192.168.65.128"
    # username = "silent"
    # password = "shangbc"
    # port = 22
    ip = "10.254.50.252"
    username = "sftp5711"
    password = "Sftp_571!"
    port = 1161
    # resetLoggin()
    sftp, client = sftpConn(ip, username, password, port)
    SearchDirFunc = SearchDir("/var/sftp/silent")
    #wirtFile(SearchDirFunc)
    # 将本地 api.py 上传至服务器 /www/test.py。文件上传并重命名为test.py
    # sftp.put("C:/Users/Lenovo/Desktop/file_code_copy.txt", "/shangbc/")

    # 将服务器 /www/test.py 下载到本地 aaa.py。文件下载并重命名为aaa.py
    # sftp.get("/var/sftp/silent/test.txt", "C:/Users/Lenovo/Desktop/test.txt")

    # 关闭连接
    client.close()

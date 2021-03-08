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
import re

# 获取1天前的当前时间
nowDate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
nowDate = datetime.datetime.strptime(nowDate, "%Y-%m-%d")
nowDateStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
nowDateLoggingStr = datetime.datetime.now().strftime("%Y%m%d")
nowDatefileStr = datetime.datetime.now().strftime("%Y%m%d%H")
# 当前路径
locatPath = sys.argv[1]
# 远程路径
SearcPath = sys.argv[2]
# 保存的文件名
fileName = sys.argv[3]
# 远程ip
ftpIp = sys.argv[4]
# 用户
ftpUser = sys.argv[5]
# 密码
ftpPassword = sys.argv[6]
# 端口
ftpPort = int(sys.argv[7])

connCount = 5


# 连接ftp
def ftpconnect(host, port, username, password):
    try:
        ftp = FTP()
        # 打开调试级别2，显示详细信息
        # ftp.set_debuglevel(2)
        ftp.connect(host, port, timeout=60)
        ftp.encoding = 'utf-8'
        ftp.set_pasv(False)
        ftp.login(username, password)
    except Exception as error:
       raise error
    else:
        return ftp


def SearchDir(path='/'):
    try:
        fileNum = 0
        ftp.cwd(path)
        files = []
        ftp.dir('.', files.append)
        for file in files:
            ftp.voidcmd("NOOP")
            if file.startswith('d'):
                nowPath = ftp.pwd() + "/" + file.split(" ")[-1]
                for bar in SearchDir(nowPath):
                    yield bar
                ftp.cwd("..")
            else:
                try:
                    fileName = file.split(" ")[-1]
                    L = list(ftp.sendcmd('MDTM ' + fileName))
                    dir_t = L[4] + L[5] + L[6] + L[7] + '-' + L[8] + L[9] + '-' + L[10] + L[11] + ' ' + L[12] + L[
                        13] + ':' + L[14] + L[
                                15] + ':' + L[16] + L[17]
                    beginDate = datetime.datetime.strptime(dir_t, "%Y-%m-%d %H:%M:%S")
                    if nowDate > beginDate:
                        logging.info("当前检索的文件：" + path + "/" + fileName)
                        yield path, fileName
                except Exception as error:
                    logging.error(file)
                    logging.error(error)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("目录：" + path + "检索完成！")


def resetLoggin():
    logPathFile = locatPath + "log/" + fileName + nowDateLoggingStr + ".log"
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename=logPathFile,
                        # a是追加模式，默认如果不写的话，就是追加模式
                        # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        filemode='a',
                        # 日志格式
                        format=
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
                        )


def fileDown(path, file):
    try:
        locaDir = "/app/cbftp/backups" + path
        if not os.path.exists(locaDir):
            os.makedirs(locaDir)
        locaFile = locaDir + "/" + file
        myFile = open(locaFile, 'wb')
        fileCodeDone = "RETR " + path + "/" + file
        ftp.retrbinary(fileCodeDone, myFile.write, 102400)

    except Exception as error:
        logging.error("文件下载失败：" + file)
        logging.error(error)
    else:
        logging.info("文件下载成功：" + file)
    finally:
        myFile.flush()
        myFile.close()


def fileDelete(SearchDirFunc):
    try:
        reCompile = re.compile(".*(\(.*\)#[0-9]+)$")
        for dirs in SearchDirFunc:
            try:
                path, file = dirs
                deleteFile = path + "/" + file
                fileSize = ftp.size(deleteFile)
                if fileSize == 0:
                    logging.info("文件大小为0：" + file)
                elif reCompile.search(file):
                    logging.info("损毁的文件：" + file)
                else:
                    logging.info("开始备份文件" + file)
                    fileDown(path, file)
                ftp.delete(deleteFile)
            except Exception as error:
                logging.error("删除文件失败：" + file)
                logging.error(error)
            else:
                logging.info("删除文件成功：" + file)

    except Exception as error:
        logging.error(error)
    finally:
        pass


if __name__ == '__main__':
    try:
        begin = time.time()
        dateTime_p = datetime.datetime.now()
        resetLoggin()
        ftp = None
        for i in range(0, connCount):
            try:
                ftp = ftpconnect(ftpIp, ftpPort, ftpUser, ftpPassword)
            except Exception as error:
                if i == 4:
                    logging.error("连接ftp失败!!")
                    raise error
                else:
                    time.sleep(10)
                    logging.info("连接ftp失败，即将重试{}次".format(i+1))
                    connCount
            else:
                logging.error("成功连接到ftp")
                break
        SearchDirFunc = SearchDir(SearcPath)
        fileDelete(SearchDirFunc)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("本次程序执行结束")
        sys.exit(0)

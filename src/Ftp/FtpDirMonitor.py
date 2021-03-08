# Time     2020/12/08 16::21
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
import time
import datetime
import logging
import threading
import sys
import os
import shutil
import json
from functools import lru_cache
from apscheduler.schedulers.blocking import BlockingScheduler

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
# 当前路径
locatPath = "E:/程序/pyhton/shangxb/src/"
# 远程路径
SearcPath = "/app/khbz"
# 保存的文件名
fileName = "FtpDirMonitor"
# 远程ip
ftpIp = "20.26.28.234"
# 用户
ftpUser = "khbz"
# 密码
ftpPassword = "khbz"
# 端口
ftpPort = int("21")

SearcPathList = ["/app/khbz/activemq", "/app/khbz/backtask", "/app/khbz/bin", "/app/khbz/deploy", "/app/khbz/ftpMonitor"
    , "/app/khbz/jdk1.8.0_141", "/app/khbz/jh-web", "/app/khbz/khbz-web", "/app/khbz/khbz-web",
                 "/app/khbz/khbz2-web", "/app/khbz/sbin", "/app/khbz/sc", "/app/khbz/sonarqube", "/app/khbz/tmp",
                 "/app/khbz/tools", "/app/khbz/xiajs", "/app/khbz/yht", "/app/khbz/zookeeper"
                 ]


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



@lru_cache(maxsize=1024)
def SearchDir(ftp, path='/'):
    try:
        fileNum = 0
        ftp.cwd(path)
        files = []
        ftp.dir('.', files.append)
        for file in files:
            ftp.voidcmd("NOOP")
            if file.startswith('d'):
                nowPath = ftp.pwd() + "/" + file.split(" ")[-1]
                for bar in SearchDir(ftp, nowPath):
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
                    logging.info("当前检索的文件：" + path + "/" + fileName)
                    # print("当前检索的文件：" + path + "/" + fileName)
                    if nowDate > beginDate:
                        fileNum += 1
                except Exception as error:
                    logging.error(file)
                    logging.error(error)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("目录：" + path + "检索完成！")
        # print("目录：" + path + "检索完成！")
        yield fileNum, nowDateStr, path


def resetLoggin():
    logPathFile = locatPath + "log/" + fileName + nowDateLoggingStr + ".log"
    # logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
    #                     filename=logPathFile,
    #                     # a是追加模式，默认如果不写的话，就是追加模式
    #                     # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
    #                     filemode='a',
    #                     # 日志格式
    #                     format=
    #                     '%(asctime)s - %(name)s - %(levelname)s - %(message)s ',
    #                     )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logHear = logging.FileHandler(logPathFile, mode='a', encoding='utf-8')
    pringHear = logging.StreamHandler()
    logHear.setLevel(logging.DEBUG)
    pringHear.setLevel(logging.DEBUG)
    formatStr = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s ')
    logHear.setFormatter(formatStr)
    pringHear.setFormatter(formatStr)
    logger.addHandler(logHear)
    logger.addHandler(pringHear)


def writFile(SearchDirFunc):
    try:
        file = fileName + nowDatefileStr + ".txt"
        locatFileName = locatPath + "file/" + file
        # ftpFile = open(locatFileName, "w")
        dictList = []
        for dirs in SearchDirFunc:
            num, myTime, path = dirs
            dicts = {
                "FTPIP": ftpIp,
                "FTPUSER": ftpUser,
                "FTPPATH": path,
                "FILENUM": num,
                "EXECTIME": myTime
            }
            dictList.append(dicts)
        jsonStr = json.dumps(dictList)
        # ftpFile.write(jsonStr)

    except Exception as error:
        logging.error("文件生成失败：")
        logging.error(error)
    finally:
        # ftpFile.flush()
        # ftpFile.close()
        pass


def execFunc(SearcPath):
    try:
        print("调用")
        ftp = ftpconnect(ftpIp, ftpPort, ftpUser, ftpPassword)
        SearchDirFunc = SearchDir(ftp, SearcPath)
        writFile(SearchDirFunc)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("本次程序执行结束")


# 程序执行时间149.5908875465393
if __name__ == '__main__':
    being = time.time()
    execFuncThreadList = []
    resetLoggin()
    # num = 0
    # for SearcPath in SearcPathList:
    #     execFuncThread = threading.Thread(target=execFunc, name='thread1' + str(num), args=(SearcPath,))
    #     execFuncThreadList.append(execFuncThread)
    #     num += 1
    #
    # for execFuncThread in execFuncThreadList:
    #     execFuncThread.start()
    #
    # for execFuncThread in execFuncThreadList:
    #     execFuncThread.join()
    execFunc(SearcPath)
    end = time.time()
    logging.info("程序执行时间" + str(end - being))

    # scheduler = BlockingScheduler()
    # scheduler.add_job(execFunc, 'cron', minute="*")
    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     pass

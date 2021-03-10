# Time     2020/12/08 16::21
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 检索ftp上未下载的文件
# edition  2021-03-10   2.0     1、更改加载配置的方式；2、加入指定目录的检索和检索的时间范围配置
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
# python 2.0
# import ConfigParser as configparser
# python 3.0以上
import configparser as configparser

# 获取1天前的当前时间
nowDateStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")



# Time     2021/03/10
# Auhtor   shangxb
# ide      PyCharm
# Verion   1.0
# function 获取配置信息
def getConfig():
    try:
        # 配置文件的路径
        configurationFile = "E://程序//pyhton//automation//src//configStatic//FtpMonitorConfig"

        # 判断文件是否存在
        fileBool = os.path.isfile(configurationFile)
        if not fileBool:
            raise FileNotFoundError("没有找到相关的配置文件：" + configurationFile)

        # 读取配置文件
        config = configparser.ConfigParser()
        # python 3.0以上
        config.read(configurationFile, encoding="utf-8")
        # python 2.0
        # config.read(configurationFile)
        return config
    except Exception as error:
        raise error


# Time     2021/03/10
# Auhtor   shangxb
# ide      PyCharm
# Verion   1.0
# function ftp连接
def ftpConnect(config):
    try:
        host, port, username, password = config['host'], int(config['port']), config['user'], config['pass']

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


# Time     2021/03/10
# Auhtor   shangxb
# ide      PyCharm
# Verion   1.0
# function 获取ftp连接
def getFtpconnect(config):
    try:
        connNum = int(config['connnum'])
        connSleep = int(config['connsleep'])
        ftp = None
        for i in range(0, connNum):
            try:
                ftp = ftpConnect(config)
            except Exception as error:
                if i == 4:
                    logging.error("连接ftp失败!!")
                    raise error
                else:
                    time.sleep(connSleep)
                    logging.error("连接ftp失败，即将重试{}次".format(i + 1))
            else:
                logging.info("成功连接到ftp")
                return ftp
    except Exception as error:
        raise error


@lru_cache(maxsize=1024)
def SearchDir(ftp, path, deleteTime):
    try:
        fileNum = 0
        ftp.cwd(path)
        files = []
        ftp.dir('.', files.append)
        for file in files:
            ftp.voidcmd("NOOP")
            if file.startswith('d'):
                nowPath = ftp.pwd() + "/" + file.split(" ")[-1]
                for bar in SearchDir(ftp, nowPath, deleteTime):
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
                    if deleteTime > beginDate:
                        fileNum += 1
                except Exception as error:
                    logging.error(error)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("目录：" + path + "检索完成！")
        # print("目录：" + path + "检索完成！")
        yield fileNum, nowDateStr, path


# 日志打印配置
def resetLoggin(config):
    # 日志文件保存的路径
    loggingFilePath = config['loggingpath']
    loggingFileName = config['loggingfilename'] + str(datetime.datetime.now().strftime("%Y%m%d")) + ".log"
    loggingFile = loggingFilePath + loggingFileName
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logFileHandler = logging.FileHandler(loggingFile, mode='a', encoding="utf-8")
    logFileHandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logFileHandler.setFormatter(formatter)
    logger.addHandler(logFileHandler)


def getWritFileData(SearchDirFunc):
    try:
        dictList = []
        for dirs in SearchDirFunc:
            num, myTime, path = dirs
            dicts = {
                "FTPIP": ftpConnFig['host'],
                "FTPUSER": ftpConnFig['user'],
                "FTPPATH": path,
                "FILENUM": num,
                "EXECTIME": myTime
            }
            dictList.append(dicts)
        return dictList

    except Exception as error:
        raise error
    finally:
        # ftpFile.flush()
        # ftpFile.close()
        pass


def writFile(dictList, filePath, fileName):
    try:
        jsonStr = json.dumps(dictList)
        # 先将数据全部生成，再写入文件，防止被上传进程和解析进程过早的取走
        file = fileName + ".txt"
        print(file)
        locatFileName = filePath + file
        ftpFile = open(locatFileName, "w")
        ftpFile.write(jsonStr)
    except Exception as error:
        logging.error("文件生成失败：")
        logging.error(error)
    finally:
        ftpFile.flush()
        ftpFile.close()


def execFunc(SearcPath, ftp, deleteTime):
    try:
        SearchDirFunc = SearchDir(ftp, SearcPath, deleteTime)
        writData = getWritFileData(SearchDirFunc)
        return writData
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("本次程序执行结束")


if __name__ == '__main__':

    configContent = getConfig()

    # log配置
    logConnFig = dict(configContent.items("LoggingConfig"))
    resetLoggin(logConnFig)

    # ftp连接配置
    ftpConnFig = dict(configContent.items("FtpConnectConfig"))
    ftp = getFtpconnect(ftpConnFig)

    # 文件检索配置
    searchFiles = dict(configContent.items("SearchFile"))
    downPath = dict(configContent.items("downPath"))['downpath']

    writDataList = []
    for searchFile in searchFiles.values():
        searchFileDict = json.loads(searchFile)
        filePath = searchFileDict['filePath']
        fileDeleteTime = searchFileDict['fileDeleteTime']
        deleteTime = (datetime.datetime.now() - datetime.timedelta(hours=fileDeleteTime)).strftime("%Y-%m-%d %H:00:00")
        deleteTime = datetime.datetime.strptime(deleteTime, "%Y-%m-%d %H:%M:%S")
        fileRegular = searchFileDict['fileRegular']
        writData = execFunc(filePath, ftp, deleteTime)
        writDataList.append(writData)

    downPath = dict(configContent.items("downPath"))['downpath']
    fileName = dict(configContent.items("downPath"))['filename']
    writFile(writDataList,downPath, fileName)


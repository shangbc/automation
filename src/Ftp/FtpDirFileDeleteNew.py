# -*- coding:UTF-8 -*-
# Time     2020/12/08 16::21
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 根据配置文件对ftp的文件进行删除

from ftplib import FTP
import time
import datetime
import logging
import json
import re
import os
# python 2.0
# import ConfigParser as configparser
# python 3.0以上
import configparser as configparser


# 获取配置信息
def getConfig():
    try:
        # 配置文件的路径
        configurationFile = "/app/cbapp/ftp_monitor/configFtpDeletaConfig"

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


# ftp连接
def ftpconnect(config):
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


# 获取ftp连接
def getFtpconnect(config):
    try:
        connNum = int(config['connnum'])
        connSleep = int(config['connsleep'])
        ftp = None
        for i in range(0, connNum):
            try:
                ftp = ftpconnect(config)
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


def SearchDir(path, deleteTime):
    try:
        fileNum = 0
        ftp.cwd(path)
        files = []
        ftp.dir('.', files.append)
        for file in files:
            ftp.voidcmd("NOOP")
            if file.startswith('d'):
                pass
            else:
                try:
                    fileName = file.split(" ")[-1]
                    L = list(ftp.sendcmd('MDTM ' + fileName))
                    dir_t = L[4] + L[5] + L[6] + L[7] + '-' + L[8] + L[9] + '-' + L[10] + L[11] + ' ' + L[12] + L[
                        13] + ':' + L[14] + L[
                                15] + ':' + L[16] + L[17]
                    beginDate = datetime.datetime.strptime(dir_t, "%Y-%m-%d %H:%M:%S")
                    if deleteTime > beginDate:
                        logging.info("当前检索的文件：" + path + "/" + fileName)
                        yield path, fileName
                except Exception as error:
                    logging.error(file)
                    logging.error(error)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("目录：" + path + "检索完成！")


# 文件备份
def fileDown(path, file, downPath):
    try:
        locaDir = downPath + path
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


# 删除ftp上的文件
def fileDelete(SearchDirFunc, downPath, fileRegular):
    try:
        reCompile = re.compile(".*(\(.*\)#[0-9]+)$")
        fileCompile = None
        if fileRegular is not None and fileRegular != "":
            fileCompile = re.compile(fileRegular)

        for dirs in SearchDirFunc:
            try:
                path, file = dirs
                if fileCompile is not None and not fileCompile.search(file):
                    continue
                deleteFile = path + "/" + file
                fileSize = ftp.size(deleteFile)
                if fileSize == 0:
                    logging.info("文件大小为0：" + file)
                elif reCompile.search(file):
                    logging.info("损毁的文件：" + file)
                else:
                    logging.info("开始备份文件" + file)
                    fileDown(path, file, downPath)
                # ftp.delete(deleteFile)
            except Exception as error:
                logging.error("删除文件失败：" + file)
                logging.error(error)
            else:
                logging.info("删除文件成功：" + file)

    except Exception as error:
        logging.error(error)
    finally:
        pass


# 程序启动入口
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
    for searchFile in searchFiles.values():
        searchFileDict = json.loads(searchFile)
        fileDeleteTime = searchFileDict['fileDeleteTime']
        deleteTime = (datetime.datetime.now() - datetime.timedelta(hours=fileDeleteTime)).strftime("%Y-%m-%d %H:00:00")
        deleteTime = datetime.datetime.strptime(deleteTime, "%Y-%m-%d %H:%M:%S")
        fileRegular = searchFileDict['fileRegular']
        SearchDirFunc = SearchDir(searchFileDict['filePath'], deleteTime)
        fileDelete(SearchDirFunc, downPath, fileRegular)

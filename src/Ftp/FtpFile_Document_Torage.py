# Time     2020/12/11 09::42
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
from ProgramConfigurationOnly import Config
import sys
import os
import re
import json
import datetime


def ftpConn():
    try:
        ftpIp = None
        ftpProt = None
        ftpUser = None
        ftpPassword = None
        ftp = FTP()
        if "FTP_IP" in conDict:
            ftpIp = conDict['FTP_IP']
        else:
            raise Exception("配置中没有配置FTP_IP")
        if "FTP_PORT" in conDict:
            ftpProt = int(conDict['FTP_PORT'])
        else:
            raise Exception("配置中没有配置FTP_PORT")
        if "FTP_USER" in conDict:
            ftpUser = conDict['FTP_USER']
        else:
            raise Exception("配置中没有配置FTP_USER")
        if "FTP_PASSWORD" in conDict:
            ftpPassword = conDict['FTP_PASSWORD']
        else:
            raise Exception("配置中没有配置FTP_PASSWORD")
        ftp.connect(ftpIp, ftpProt)
        ftp.encoding = 'utf-8'
        ftp.login(ftpUser, ftpPassword)
        # ftp.set_pasv(False)
    except Exception as error:
        logging.error(error)
    else:
        logging.info("ftp连接成功！")
        return ftp


def fileDown(locaDir, file):
    try:
        locaFile = locaDir + file
        myFile = open(locaFile, 'wb')  #
        fileCodeDone = "RETR " + file
        ftp.retrbinary(fileCodeDone, myFile.write)

    except Exception as error:
        logging.error(error)
    else:
        logging.info(file + "下载成功！")
    finally:
        myFile.flush()
        myFile.close()


def fileUp(locaDir, filePathHis, file, filePath):
    try:
        locaFile = locaDir + file
        myFile = open(locaFile, 'rb')  #
        fileCodeUp = "STOR " + filePathHis + "/" + file
        ftp.storbinary(fileCodeUp, myFile)
    except Exception as error:
        logging.error(error)
    else:
        logging.info(file + "转移历史目录成功！")
        ftp.delete(filePath + "/" + file)
    finally:
        myFile.flush()
        myFile.close()
        os.remove(locaFile)


def fileAnalysis(locaDir, file):
    try:
        locaFile = locaDir + file
        myFile = open(locaFile, 'r')
        lineList = myFile.read()
        fileJsons = json.loads(lineList)
        data = []
        for fileJson in fileJsons:
            fileJson['EXECTIME'] = datetime.datetime.strptime(fileJson['EXECTIME'], '%Y-%m-%d %H:%M:%S')
            data.append(fileJson)
        sql = "insert into cboss.cboss_ftp_path_nums(FTPIP,FTPUSER,FTPPATH, FILENUM, execTime) values(:FTPIP,:FTPUSER," \
              ":FTPPATH, :FILENUM, :EXECTIME)"
        ora.batchExec("cboss", sql, data)
    except Exception as error:
        logging.error(error)
        raise error
    else:
        ora.dataCommit()


def ftpFileExec():
    try:
        filePath = None
        filePathHis = None
        locaPath = None
        reFile = None
        if "FTP_FILE_PATH" in conDict:
            filePath = conDict['FTP_FILE_PATH']
        else:
            raise Exception("配置中没有配置FTP_FILE_PATH")
        if "FTP_FILE_PATH_HIS" in conDict:
            filePathHis = conDict['FTP_FILE_PATH_HIS']
        else:
            raise Exception("配置中没有配置FTP_FILE_PATH_HIS")
        if "LOCA_FILE_PATH" in conDict:
            locaPath = conDict['LOCA_FILE_PATH']
        else:
            raise Exception("LOCA_FILE_PATH")
        if "RE_FILE" in conDict:
            reFile = conDict['RE_FILE']
        else:
            raise Exception("RE_FILE")
        ftp.cwd(filePath)
        fileList = ftp.nlst()
        sysPath = sys.argv[0]
        nowPath = os.path.dirname(sysPath)

        if not os.path.exists(nowPath + locaPath):
            os.mkdir(nowPath + locaPath)
        reCompile = re.compile(reFile)
        locaDir = nowPath + locaPath + "/"
        for file in fileList:
            try:
                fileBool = False
                try:
                    ftp.cwd(file)
                except Exception as error:
                    fileBool = True
                else:
                    ftp.cwd("..")
                if not fileBool:
                    continue
                # 判断是不是目标文件
                if reCompile.search(file) is None:
                    continue
                # 文件下载到本地
                fileDown(locaDir, file)
                # 文件解析入库
                fileAnalysis(locaDir, file)
                # 文件转移历史目录
                fileUp(locaDir, filePathHis, file, filePath)
            except Exception as error:
                logging.error("文件解析失败：" + file)
            else:
                logging.info("文件解析成功：" + file)
            finally:
                pass
                # myFile.close()
                # ftp.cwd(filePath)

    except Exception as error:
        logging.error(error)


if __name__ == '__main__':
    config = Config("FTPFILE_DOCUMENT_TORAGE")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    ftp = ftpConn()
    ftpFileExec()

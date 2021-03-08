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

# 获取1天前的当前时间
nowDate = datetime.datetime.now() - datetime.timedelta(days=1)


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


def ergodic(path='/'):
    fileCount = 0
    ftp.cwd(path)
    files = []
    ftp.dir('.', files.append)
    for file in files:
        ftp.voidcmd("NOOP")
        if file.startswith('d'):
            path = ftp.pwd() + "/" + file.split(" ")[-1]
            ergodic(path)
            ftp.cwd("..")
        else:
            try:
                fileName = file.split(" ")[-1]
                L = list(ftp.sendcmd('MDTM ' + fileName))
                dir_t = L[4] + L[5] + L[6] + L[7] + '-' + L[8] + L[9] + '-' + L[10] + L[11] + ' ' + L[12] + L[
                    13] + ':' + L[14] + L[
                            15] + ':' + L[16] + L[17]
                beginDate = datetime.datetime.strptime(dir_t, "%Y-%m-%d %H:%M:%S")
                nowDate = datetime.datetime.now() - datetime.timedelta(days=2)
                if nowDate > beginDate:
                    fileCount += 1
            except Exception as error:
                logging.error(file)
    writeStr = "10.254.50.249|" + "FCSV5711|" + ftp.pwd() + "|" + str(fileCount) + "|" + timeStrSc + "\n"
    logging.info(writeStr)
    fo.write(writeStr)
    fo.flush()


def fileUp(file):
    try:
        locaFile = "/app/cbapp/ftp_monitor/file/" + file
        locaFileHis = "/app/cbapp/ftp_monitor/file/his/" + file
        myFile = open(locaFile, 'rb')  #
        fileCodeUp = "STOR " + "/app/khbz/ftpMonitor/" + file
        khbzFtp.storbinary(fileCodeUp, myFile)
    except Exception as error:
        logging.error(error)
    else:
        myFile.flush()
        myFile.close()
        shutil.move(locaFile, locaFileHis)
        # os.remove(locaFile)
        logging.info(file + "监控文件上传20.26.28.234成功！")
    finally:
        # myFile.flush()
        # myFile.close()
        pass


if __name__ == '__main__':
    begin = time.time()
    dateTime_p = datetime.datetime.now()
    logStr = datetime.datetime.strftime(dateTime_p, '%Y%m%d')
    timeStr = datetime.datetime.strftime(dateTime_p, '%Y%m%d%H')
    timeStrSc = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logPathFile = "/app/cbapp/ftp_monitor/log/" + "FTPFILE_NUM_FCSV5711_" + logStr + ".log"
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename=logPathFile,
                        filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        # a是追加模式，默认如果不写的话，就是追加模式
                        format=
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
                        )
    # 日志格式

    fileName = "FTPFILE_NUM_FCSV5711_" + timeStr + ".txt"
    locaName = '/app/cbapp/ftp_monitor/file/' + fileName
    # ftp = ftpconnect("20.26.28.234", 21, "khbz", "khbz!")
    ftp = ftpconnect("10.254.50.249", 1160, "fcsv5711", "Ftp5711!")

    fo = open(locaName, "wb")
    ergodic('/incoming')
    fo.close()
    ftp.close()

    khbzFtp = ftpconnect("20.26.28.234", 21, "khbz", "khbz")
    fileUp(fileName)

    end = time.time()
    logging.info(end - begin)
    sys.exit(0)

# Time     2021/01/29 09::20
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function oao实名认证文件补传文件重入库处理
from ftplib import FTP
from ProgramConfigurationOnly import Config
import time
import datetime
import logging
import sys
import os
import re

# 获取1天前的当前时间
nowDate = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
nowDate = datetime.datetime.strptime(nowDate, "%Y-%m-%d %H:00:00")
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


# 远程路径
SearcPath = "/app/ftpuser/outgoing/OSP/pic/571/tmp"
# SearcPath = "/app/khbz"
# 远程ip
ftpIp = "10.78.220.32"
# 用户
ftpUser = "ftpuser"
# 密码
ftpPassword = "ftpuser"
# 端口
ftpPort = 21

connCount = 5

reCompile = re.compile("BOSS5711085.*(\.jpg)$")


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
                # nowPath = ftp.pwd() + "/" + file.split(" ")[-1]
                # for bar in SearchDir(nowPath):
                #     yield bar
                # ftp.cwd("..")
                pass
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


def reprocessing(path, file):
    try:
        dictFileName = {
            "fileName": file
        }
        sql = """
              select * from cboss.ftp_file_interface t
              where t.file_name = '[:fileName]'
              """
        fileData = ora.selectSqlExecute("cboss", sql, dictFileName)
        fileNum = len(fileData)
        execSql = ""
        if fileNum == 0:
            logging.info("插入："+file)
            execSql = """
                      insert into cboss.ftp_file_interface(FILE_TYPE,file_name,proc_status,file_status,process_date,create_date,file_way,total_amount,notes)
                      values('7711','[:fileName]',0,0,sysdate,sysdate,1,0,'文件解析入库成功')
                      """
        elif int(fileData[0]['proc_status']) == 1:
            logging.info("修改：" + file)
            execSql = """
                      update cboss.ftp_file_interface t
                      set t.proc_status = 0
                      where t.file_name = '[:fileName]'
                      """

        ora.sqlExecute("cboss", execSql, dictFileName)
    except Exception as error:
        logging.error("修改处理状态失败：" + file)
        logging.error(error)
        ora.dataRollback()
    else:
        logging.info("修改处理状态成功：" + file)
        # ora.dataRollback()
        ora.dataCommit()



def fileDelete(SearchDirFunc):
    try:
        i = 0
        for dirs in SearchDirFunc:
            i += 1
            try:
                path, file = dirs
                deleteFile = path + "/" + file
                logging.info(file)
                if reCompile.search(file):
                    reprocessing(path, file)
            except Exception as error:
                logging.error(error)

    except Exception as error:
        logging.error(error)
    finally:
        pass


if __name__ == '__main__':
    try:
        config = Config("OaoFileReprocessing")
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()
        begin = time.time()
        dateTime_p = datetime.datetime.now()
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
                logging.info("成功连接到ftp")
                break
        SearchDirFunc = SearchDir(SearcPath)
        fileDelete(SearchDirFunc)
    except Exception as error:
        logging.error(error)
    finally:
        logging.info("本次程序执行结束")
        sys.exit(0)

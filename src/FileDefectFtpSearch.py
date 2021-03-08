# Time     2021/01/07 08::22
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
from configparser import ConfigParser
from ProgramConfigurationOnly import Config
import os
import sys
import datetime


# 读取配置
def getStaticConfig():
    staticConfig = ConfigParser()
    staticConfig.read("E:/程序/pyhton/shangxb/src/configStatic/ftpConfig.ini", encoding="utf-8")
    return staticConfig


# 获取ftp连接
def getFtpConnect(ftpHost, ftpPort, ftpUser, ftpPass):
    # 创建端口
    ftpConn = FTP()
    # 设置ip和端口号
    ftpConn.connect(ftpHost, ftpPort, 100)
    # 编码格式
    ftpConn.encoding = 'utf-8'
    # 调试级别
    ftpConn.set_debuglevel(0)
    # 主动/被动模式
    ftpConn.set_pasv(True)
    # 登录
    ftpConn.login(ftpUser, ftpPass)
    return ftpConn


# 获取超过1天工单状态还未从初始状态转为待外呼的数据
def dataSearch():
    try:
        # 3天内文件校验不通过就会返回文件缺失
        sql = """
              select * from  bomc.ord_cross_user_destory_check t
              where t.state = 'Y'
                    and t.exe_code in ('01','02')
              """
        arrayList = ora.selectSqlExecute("backTable", sql)
        return arrayList
    except Exception as error:
        raise error


def ftpFileDoenSearch(fileName):
    # 待定，计划将ftp上的文件读入表中
    pass


# 判断文件是否存在
def ftpFileCheck(ftp, file, downPath):
    downFile = os.path.join(downPath, file).replace("\\", "/")
    mdtm = "MDTM " + downFile
    checkBoll = True
    code = ""
    try:
        # 获取文件时间， 获取失败即为不存在
        dataList = list(ftp.sendcmd(mdtm))
        time = " ".join([
            "-".join(["".join(dataList[4:8]), "".join(dataList[8:10]), "".join(dataList[10:12])]),
            ":".join(["".join(dataList[12:14]), "".join(dataList[14:16]), "".join(dataList[16:18])])
        ])
        time = datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        nowDate = datetime.datetime.now() - datetime.timedelta(hours=1)
        # 根据文件时间判断是否处理进程出了问题
        if time <= nowDate:
            code = True
        else:
            code = False

    except Exception as error:
        checkBoll = False
        logging.error(error)
    else:
        logging.info(file)
    finally:
        return checkBoll, code


#
def ftpFileMove(hisPath, newPath, file, ftp, ftp2=None):
    try:
        if ftp2 is None:
            ftp2 = ftp
        locatPath = os .path .dirname(sys.argv[0])
        locatFile = os .path .join(locatPath, "file", file) .replace("\\", "/")
        dowmFile = os .path .join(hisPath, file) .replace("\\", "/")
        moveFile = os .path .join(newPath, file) .replace("\\", "/")
        retr = "RETR " + dowmFile
        stor = "STOR " + moveFile
        # 文件下载
        ftpFile = open(locatFile, "wb")
        ftp2 .retrbinary(retr, ftpFile.write)
        ftpFile.flush()
        ftpFile .close()

        # 文件上传
        ftpFileup = open(locatFile, "rb")
        ftp .storbinary(stor, ftpFileup)
        ftpFileup .close()

        # 删除历史文件，暂定
        # ftp.delete(dowmFile)

    except Exception as error:
        logging.error(error)
    finally:
        ftpFile.close()
        ftpFileup.close()
        os .remove(locatFile)


#
def ftpFileCheckSearch(dataVaule):
    # 人脸检验缺失通常问文件积压校验时有问题
    host = "10.78.220.32"
    port = 21
    user = "ftpuser"
    password = "ftpuser"
    host2 = "10.78.145.56"
    port2 = 21
    user2 = "ftpuser"
    password2 = "ftpuser"
    path = {
        "downLocat": "/app/cbftp/outgoing/10085/pic/cancelsyncDOWN/tmp",
        "downHisLocat": "app/cbftp/backups/outgoing/KQFW",
        "checkPath": "/app/cbftp/outgoing/10085/pic/cancelsyncDOWN",
        "checkHisPath": "/app/cbftp/outgoing/10085/pic/cancelsyncDOWN/his"
    }
    ftp = getFtpConnect(host, port, user, password)
    ftp2 = getFtpConnect(host, port, user, password)
    fileList = eval(dataVaule['EXE_ERROR'])
    code = "00"
    doce = {}
    for file in fileList:
        # 下载目录文件检测，若存在即为解密有问题
        doce[file] = "文件丢失！！"
        boole, codeBoll = ftpFileCheck(ftp, file, path['downLocat'])

        if boole and codeBoll:
            code = "04" if code != "06" else code
            doce[file] = "文件积压在下载目录，请检查解密进程是否有问题！！！"
            continue

        # # 是否被其他进程下载
        # boole, codeBoll = ftpFileCheck(ftp, file, path['downHisLocat'])
        # if boole:
        #     code = "05" if code != "06" else code
        #     doce[file] = "文件积被其他进程下载，文件自动移回当前目录！！"
        #     ftpFileMove(path['downHisLocat'], path['downLocat'], file, ftp, ftp2)
        #     continue

        # 人脸识别目录检测，若存在即为校验有问题
        boole, codeBoll = ftpFileCheck(ftp, file, path['checkPath'])
        if boole and codeBoll:
            code = "04" if code != "06" else code
            doce[file] = "文件积压在校验目录，请检查校验进程是否有问题！！！"
            continue

            # 人脸识别历史目录，若存在即为检验有问题
        boole, codeBoll = ftpFileCheck(ftp, file, path['checkHisPath'])
        if boole:
            code = "05" if code == "00" else code
            doce[file] = "文件已经校验过，但表中无数据，已重新校验！"
            ftpFileMove(path['checkHisPath'], path['checkPath'], file, ftp)
            continue
        code = "06"

    return code, doce


def data(dataSearchFunc):
    for vaule in dataSearchFunc:
        try:
            code, doce = "", ""
            exeCode = vaule['EXE_CODE']
            # 文件下载记录不完全，检查ftp上文件是否存在
            if exeCode == '01':
                pass
            # 文件下载正常，人脸识别校验缺失，检查主机上文件是否存在
            elif exeCode == '02':
                code, doce = ftpFileCheckSearch(vaule)
            if code == "" or doce == "":
                continue
            dictVaule = [{
                "exeCode": code,
                "exeError": str(doce),
                "oprNumber": vaule['opr_number']
            }]
            sql = """
                  update bomc.ord_cross_user_destory_check t
                  set t.check_date = sysdate,
                      t.exe_error = :exeError,
                      t.exe_code = :exeCode
                  where opr_number = :oprNumber
                  """
            sql = ora.strFormat(sql)
            ora.batchExec("backTable", sql, dictVaule)
        except Exception as error:
            logging.error(error)
            ora.dataRollback()
        else:
            ora.dataCommit()


if __name__ == '__main__':
    # config = getStaticConfig()
    # host = config.get("ftp", "host")
    # port = config.getint("ftp", "port")
    # user = config.get("ftp", "user")
    # password = config.get("ftp", "pass")

    config = Config("AccountCancellationSearch")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    dataSearchFunc = dataSearch()
    data(dataSearchFunc)
    # host = 20.26.28.234
    # port = 21
    # user = "khbz"
    # password = "khbz"
    #

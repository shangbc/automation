# Time     2021/01/06 16::32
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 检查销户文件是否有下载记录
from ProgramConfigurationOnly import Config
import sys


# 获取超过1天工单状态还未从初始状态转为待外呼的数据
def dataSearch():
    try:
        # 3天内文件校验不通过就会返回文件缺失
        sql = """
              select * from  bomc.ord_cross_user_destory_check t
              where t.state = 'Y'
                    and (t.exe_code = '00' or t.exe_code is null)
                    
              """
        arrayList = ora.selectSqlExecute("backTable", sql)
        return arrayList
    except Exception as error:
        raise error


# 文件下再情况
def fileDownCheck(dataSearch):
    try:
        check = True
        error = []
        fileNameList = []
        fileNameList.append(dataSearch['PICTRUE_T'])
        fileNameList.append(dataSearch['CREDIT_PICTURE'])
        fileNameList.append(dataSearch['LIST_PICTURE'])
        fileNameList.append(dataSearch['ELCTRIC_ORDER'])
        for fileName in fileNameList:
            dictVaule = {
                "file_name": fileName
            }
            sql = """
                  select count(1) num from cboss.ftp_file_interface t
                  where t.file_name = '[:file_name]'
                  """
            vaule = ora.selectSqlExecute("cboss", sql, dictVaule)
            if int(vaule[0]['num']) == 0:
                error.append(fileName)
                check = False
        return check, error
    except Exception as error:
        raise error


# @Time    2020/10/10 11:33:00
# Auhtor   shangxb
# Verion   1.0
# function 根据地市编号返回对应的用户和连接名
def regionCodes(region):
    # 存放用户
    vso = None
    # 存放数据源名
    vsoDb = None
    if region == 571:
        vso = 'so1'
        vsoDb = 'crma'
    elif region == 572:
        vso = 'so572'
        vsoDb = 'crme'
    elif region == 570 or region == 574 or region == 579:
        vso = 'so2'
        vsoDb = 'crmb'
    elif region == 577 or region == 578 or region == 580:
        vso = 'so3'
        vsoDb = 'crmc'
    elif region == 573 or region == 575 or region == 576:
        vso = 'so4'
        vsoDb = 'crmd'
    else:
        logging.info("地市编号有误")
        raise Exception("地市编号有误")
    return vso, vsoDb


# 人脸识别校验情况
def fileCheck(dataSearch):
    try:
        check = True
        error = []
        user, db = regionCodes(int(dataSearch['region_id']))
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")
        dictVaule = {
            "user": user,
            "region": dataSearch['region_id'],
            "compareId": dataSearch['compare_id']
        }
        sql = """
              select * from [:user]ordx_face_compare_[:region] t
              where t.compare_id = [:compareId]
              """
        vaule = ora.selectSqlExecute(db, sql, dictVaule)
        if len(vaule) == 0:
            check = False
            return check, error

        if vaule[0]['CERT_PHOTO_NAME_Z'] is None:
            error.append(dataSearch['CREDIT_PICTURE'])
            check = False
        if vaule[0]['CERT_PHOTO_NAME_H'] is None:
            error.append(dataSearch['PICTRUE_T'])
            check = False
        if vaule[0]['CERT_PHOTO_NAME_F'] is None:
            error.append(dataSearch['LIST_PICTURE'])
            check = False
        if vaule[0]['CERT_PHOTO_NAME_H_1'] is None:
            error.append(dataSearch['ELCTRIC_ORDER'])
            check = False
        # CERT_PHOTO_NAME_Z
        return check, error
    except Exception as error:
        raise error


def data(dataSearchFunc):
    for dataSearch in dataSearchFunc:
        try:
            fileNameList = []
            exeCode = ""
            # 校验文件下载记录
            check, error = fileDownCheck(dataSearch)
            # 当文件下载正常时校验人脸识别记录
            if check:
                check, error = fileCheck(dataSearch)
                if check:
                    exeCode = "03"
                else:
                    exeCode = "02"
            else:
                exeCode = "01"

            dictVaule = [{
                "exeCode": exeCode,
                "exeError": str(error),
                "oprNumber": dataSearch['opr_number']
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
    # 程序配置在cboss.python_config_cboss表中
    config = Config("AccountCancellationSearch")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    try:
        dataSearchFunc = dataSearch()
        data(dataSearchFunc)
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        sys.exit(0)
    finally:
        ora.dataClose()

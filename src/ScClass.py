# Time     2021/01/20 16::56
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import pandas
import numpy
import datetime
import copy


def func():
    sqlDict = {
        "yyyymm": datetime.datetime.now().strftime("%Y%m")
    }
    sql = """
          select * from bomc.cboss_interface_data_[:yyyymm] t
          where t.index_code = 'BACKLOG_COUNT'
                and t.create_time < trunc(sysdate,'mi')
                and t.create_time >= trunc(sysdate-1/1440,'mi')
          order by t.create_time desc
          """
    listVaule = ora.selectSqlExecute("backTable", sql, sqlDict)
    if len(listVaule) == 0:
        return

    for vaule in listVaule:
        state = 0
        if float(vaule['INDEX_VAULE']) == 0.0:
            state = 0
        else:
            dict = {
                "link_name": vaule['link_name'],
                "city_code": vaule['city_code'],
                "index_code": vaule['index_code'],
                "interface_code": vaule['interface_code']
            }
            sql = """
                  select * from bomc.cboss_interface_data_[:yyyymm] t
                  where t.index_code = 'BACKLOG_COUNT'
                        and t.link_name = '[:link_name]',
                        and t.interface_code = '[:interface_code]'
                        and t.city_code = '[:city_code]',
                        and t.index_code = '[:index_code]'
                        and t.index_vaule <> '0'
                        and t.create_time < trunc(sysdate,'mi')
                        and t.create_time >= trunc(sysdate-5/1440,'mi')
                  order by t.create_time desc
                  """
            dataList = ora.selectSqlExecute("backTable", sql, dict)
            num = len(dataList)
            if num <= 3:
                state = 1
            else:
                state = 2

        dictData = copy.deepcopy(vaule)
        dictData["NUMS"] = int(dictData['success_nums'] + dictData['BUSINESS_FAIL_NUMS'])
        dictData['INDEX_LINK'] = dictData['LINK_NAME']
        dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
        dictData['INDEX_VAULE'] = 0
        dictData['INDEX_LABEL'] = state
        dictData.pop('success_nums')
        dictData.pop('BUSINESS_FAIL_NUMS')
        dictData.pop('SYSTEM_FAIL_NUMS')
        dictData.pop('LINK_NAME')
        yield dictData


def operation():
    aa = []
    for fun in func():
        a = fun
        aa.append(a)
    return aa


# 数据写入数据库
def write():
    try:
        esbs = None
        if "DATA" in conDict:
            esbs = conDict['DATA']
        else:
            raise Exception("接口信息没有配置")
        esbs = eval(esbs)
        aa = operation()

        if len(aa) == 0:
            return

        table = "bomc.OAOVIEW_DATA_LABEL_" + datetime.datetime.now().strftime("%Y%m")
        sql, vauleList = ora.batchInsertSql(table, aa, "backTable")
        ora.batchExec("backTable", sql, vauleList)
    except Exception as error:
        logging.error(error)
        ora.dataRollback()
    else:
        ora.dataCommit()


if __name__ == '__main__':
    config = Config("EsbInterFaceDate")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    write()

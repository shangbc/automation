# Time     2020/12/31 16::07
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 跨区销户文件缺失工单提取

from ProgramConfigurationOnly import Config
import time
import sys
import copy


# 获取超过1天工单状态还未从初始状态转为待外呼的数据
def dataSearch(user, vsoDb, region):
    try:
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")
        # 3天内文件校验不通过就会返回文件缺失
        vaule = {
            "user": user,
            "region": str(region)
        }
        sql = """
              select t.opr_number,
                     t.pictrue_t,
                     t.credit_picture,
                     t.list_picture,
                     t.elctric_order,
                     t.region_id,
                     t.create_date,
                     t.compare_id,
                     substr(t.pictrue_t,5,3) as Region_id_other,
                     'Y' as state,
                     '00' as exe_code
              from  [:user]ORD_CROSS_USER_DESTORY_[:region] t
              where sysdate < t.create_date + 3
                    and sysdate >= t.create_date + 1
                    and t.state = 0
              """
        arrayList = ora.selectSqlExecute(vsoDb, sql, vaule)
        yield arrayList
    except Exception as error:
        logging.error(error)


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


def regionExecute():
    # 循环11个地市
    for region in range(570, 581):
        # 存放用户
        vso = None
        # 存放数据源名
        vsoDb = None
        # 根据地市编号返回用户名和数据库连接名
        user, vsoDb = regionCodes(region)
        yield from dataSearch(user, vsoDb, region)
    del vso, vsoDb


def data(dataSearchFunc):
    lists = []
    for dataSearch in dataSearchFunc:
        lists.extend(copy.deepcopy(dataSearch))
        del dataSearch
    return lists


def dataRemoval(dataList):
    try:
        oprNumberList = [value['OPR_NUMBER'] for value in dataList]
        oprNumber = "('" + "','".join(oprNumberList) + "')"
        dictVaule = {
            "oprNumber": oprNumber
        }
        sql = """
              select t.opr_number from bomc.ord_cross_user_destory_check t
              where t.opr_number in [:oprNumber]
              """
        valueList = ora.selectSqlExecute("backTable", sql, dictVaule)
        oprNumberId = [value['OPR_NUMBER'] for value in valueList]

        # 剔除重复
        for value in dataList[::-1]:
            if value['OPR_NUMBER'] in oprNumberId:
                dataList.remove(value)
    except Exception as error:
        raise error
    return dataList


# 数据插入
def dataInsert(dataList):
    try:
        if len(dataList) == 0:
            return
        dbpInsertSql, keyList = ora.batchInsertSql("bomc.ord_cross_user_destory_check", dataList, "backTable")
        ora.batchExec("backTable", dbpInsertSql, keyList)
    except Exception as error:
        ora.dataRollback()
        raise error
    else:
        ora.dataCommit()
        ora.dataClose()


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    config = Config("AccountCancellationSearch")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    try:
        dataSearchFunc = regionExecute()
        # 获取数据
        arrayList = data(dataSearchFunc)
        arrayList = dataRemoval(arrayList)
        dataInsert(arrayList)
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        sys.exit(0)
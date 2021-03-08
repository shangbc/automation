import Dbutil
import logging
import sys
import time
from RegionUtil import regionCodes
from LogUtil import LogUtil
from func_timeout import func_set_timeout
import func_timeout

def regionCodes(region):
    # 存放用户
    vso = None
    # 存放数据源名
    vsoDb = None
    if region == 571:
        vso = 'so1'
        vsoDb = 'crma'
    elif region == 572:
        vso = ''
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


# @Time    2020/09/10 11:33:00
# Auhtor   shangxb
# Verion   1.0
# function OAO激活失败，自动修改为DF，自愈
def boeCbossOaoJhsbZiyu(vso, vsoDb, region):
    try:
        # select * from sox.ord_interate_subord_sync_xxx a
        # where done_date >trunc(sysdate)
        #       and done_date<sysdate-1/1440
        #       and error_msg like 'H04-激活不成功%'
        #       and error_msg!='H04-激活不成功,证件号码被预占'
        #       and state='AF'
        #       and (n_ext_1 is null or  n_ext_1<9)
        # 分批执行，每次获取100条数据
        # ------------------------begin----------------------------------------------------------------------
        # --------------

        # 判断是否需要输入用户名
        regionCode = str(vso) if vso == '' else (str(vso) + ".")

        # DADBd如果想使用rownum的话对查询的数据量有一定的要求，所以此处先对数据量进行统计，然后根据count(1)的量来拼接对应的rownum语句
        crmSelectSql1 = "select count(*) as countNum from " + regionCode + "ord_interate_subord_sync_" + str(region) + \
                        " a where done_date >trunc(sysdate) " \
                        " and done_date<sysdate-1/1440 " \
                        " and error_msg like 'H04-激活不成功%' " \
                        " and error_msg!='H04-激活不成功,证件号码被预占' " \
                        " and state='AF' " \
                        " and (n_ext_1 is null or  n_ext_1<9)"
        crmOrdValues = ora.selectSqlExecute(vsoDb, crmSelectSql1)

        # 如果数据量为0时使用rownum sql查询会直接报错，所以此处直接终止本次执行，进行下一个地市的数据处理
        if int(crmOrdValues[0]["countNum"]) == 0:
            logging.info("当前没有需要修复的数据")
            return 0

        # 根据待处理的数据量来判断分页的条件
        crmSelectSql = "select a.*,sysdate as CTDATE from " + regionCode + "ord_interate_subord_sync_" + str(region) + \
                       " a where done_date >trunc(sysdate) " \
                       " and done_date<sysdate-1/1440 " \
                       " and error_msg like 'H04-激活不成功%' " \
                       " and error_msg!='H04-激活不成功,证件号码被预占' " \
                       " and state='AF' " \
                       " and (n_ext_1 is null or  n_ext_1<9)" + \
                       (" and rownum <= 100" if int(crmOrdValues[0]["countNum"]) >= 100 else " and rownum >= 0")

        # 获取数据集
        crmOrdValues = ora.selectSqlExecute(vsoDb, crmSelectSql)
        # ---------------
        # -----------------------end------------------------------------------------------------------------------
        rowCount = len(crmOrdValues)
        # 判断获取数据集是否为空
        if rowCount == 0:
            logging.info("当前没有需要修复的数据")
            return rowCount

        # 将ordInterateubord_sync_中的流水号order_id单提出来进行拼装，作为查询语句的条件
        orderId = ""
        for crmOrdValue in crmOrdValues:
            orderId += "'" + crmOrdValue['ORDER_ID'] + "',"
        orderId = orderId[0:len(orderId) - 1]

        # select * from sox.ord_interate_subord_sync_xxx a
        # where done_date >trunc(sysdate)
        #       and done_date<sysdate-1/1440
        #       and error_msg like 'H04-激活不成功%'
        #       and error_msg!='H04-激活不成功,证件号码被预占'
        #       and state='AF'
        #       and (n_ext_1 is null or  n_ext_1<9)
        #       and order_id not in  (select b.order_id from  bomc.ord_interate_subord_sync_zy b )
        # 防止bomc.ord_interate_subord_sync_zy数据量过大导致内存溢出，这里将数据集拼装成sql进行剔除查询，返回需要处理的crmOrdValuesList结果集
        # -----------------------------begin-----------------------------------------------------------------
        # ---------------
        pdbSelectSql = "select order_id from ord_interate_subord_sync_zy " + \
                       "where order_id in(" + orderId + ")"
        dbpOrdValues = ora.selectSqlExecute(pdbLink, pdbSelectSql)
        dbpOrdValuesList = []
        crmOrdValuesList = []
        # 将ord_interate_subord_sync_zy获取的的结果单独存放于列表中，方便匹配数据
        for dbpOrdValue in dbpOrdValues:
            dbpOrdValuesList.append(str(dbpOrdValue['ORDER_ID']))
        # 将ordInterateubord_sync_中的流水号order_id单提出来进行拼装，作为查询语句的条件
        # 剔除已经处理过的数据，并将未处理的整合成一个列表
        for crmOrdValue in crmOrdValues:
            if type(crmOrdValue['ORDER_ID']) is not None and crmOrdValue['ORDER_ID'] not in dbpOrdValuesList:
                crmOrdValuesList.append(crmOrdValue)
        # ----------------
        # ----------------------------end--------------------------------------------------------------------

        # 判断当前处理的数据是需要备份
        crmOrdValuesNum = len(crmOrdValuesList)
        if crmOrdValuesNum == 0:
            logging.info("当前没有需要备份的数据")
        else:
            # 记录相关的治愈信息
            dbpRptInsertSql = "insert into RPT_GW_AUTOFIX values ('CBOSS-OAO激活失败自动修改为DF','陈斌',trunc(sysdate)," + \
                              str(crmOrdValuesNum) + ",sysdate)"
            ora.sqlExecute(pdbLink, dbpRptInsertSql)
            # 数据备份
            # -------------------------------begin-----------------------------------------------------------
            # 传入表名和数据集，方法会返回insert语句和处理后的数据集
            sql, keyList = ora.batchInsertSql("ord_interate_subord_sync_zy", crmOrdValuesList)
            # 执行批量插入
            ora.batchInsert(pdbLink, sql, keyList)
            # --------------------------------end------------------------------------------------------------

        # 修改已经治愈的数据的状态
        crmUpdateSqlOne = "update " + regionCode + "ord_interate_subord_sync_" + str(region) + \
                          " a set a.state='DF',A.ACTIVE_FLAG='2',a.n_ext_1=4  " \
                          "where done_date >trunc(sysdate) " \
                          " and done_date<sysdate-1/1440 " \
                          " and error_msg like 'H04-激活不成功%' " \
                          " and error_msg!='H04-激活不成功,证件号码被预占' " \
                          " and state='AF' " \
                          " and (n_ext_1 is null ) " \
                          " and order_id in (" + orderId + ")"
        crmUpdateSqlTwo = "update " + regionCode + "ord_interate_subord_sync_" + str(region) + \
                          " a set a.state='DF',A.ACTIVE_FLAG='2',a.n_ext_1=4  " \
                          "where done_date >trunc(sysdate) " \
                          " and done_date<sysdate-1/1440 " \
                          " and error_msg like 'H04-激活不成功%' " \
                          " and error_msg!='H04-激活不成功,证件号码被预占' " \
                          " and state='AF' " \
                          " and (n_ext_1 <9 ) " \
                          " and order_id in (" + orderId + ")"

        ora.sqlExecute(vsoDb, crmUpdateSqlOne)
        ora.sqlExecute(vsoDb, crmUpdateSqlTwo)

    except Exception as errorCode:
        logging.info(errorCode)
        logging.info("异常输出并回滚数据")
        ora.dataRollback()
        ora.dataClose()
        # 避免报错中出现单引号引起记录插入失败
        errorCode = str(errorCode).replace("'", "''")
        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='3',
                      taskLog=str(region) + "地市执行失败",
                      errMsg=str(errorCode))
        sys.exit(1)
    else:
        logging.info("执行成功，保存数据")
        ora.dataCommit()
        return rowCount


# Time    2020/09/16 11:33:00
# Author   ShangXb
# Version   1.0
# Function  循环11地市，判断每个地市对应的用户和数据源，然后根据数据源和用户调用处理的方法
#           func_set_timeout（10*60）  正常情况下数据处理时间不会超过10分钟，如果程序执行时间超过10分钟，程序自动抛出超时异常
@func_set_timeout(10 * 60)
def regionExecute():
    # 循环11个地市
    for region in range(570, 581):
        # 存放用户
        vso = None
        # 存放数据源名
        vsoDb = None

        # 根据地市编号返回用户名和数据库连接名
        vso, vsoDb = regionCodes(region)

        logging.info(str(region) + "地市开始执行")
        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='0',
                      taskLog=str(region) + "地市开始执行")
        regionStartTime = time.time()
        rowCount = boeCbossOaoJhsbZiyu(vso, vsoDb, region)
        rowCount = rowCount if rowCount is not None else 0
        regionEndTime = time.time()
        regionExeTime = regionEndTime - regionStartTime
        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='0',
                      taskLog=str(region) + "地市执行完成", doneDate=str(regionExeTime)[0:5] + "s", rowCount=rowCount)
    del vso, vsoDb


# 主程序运行入口
if __name__ == '__main__':
    # 避免两个模块之间相互引入出现错误


    # 初始化数据源使用的字典
    # 程序中识别名称：创建时使用的源数据名称
    dbLinkName = {
        "crma": "yya",
        "crmb": "yyb",
        "crmc": "yyc",
        "crmd": "yyd",
        "crme": "572_yye_dadb",
        # "pdb": "pdb_larkbird",
        "crmkf": "crmkfdb_prod"
    }
    log = LogUtil()
    # 记录日志
    log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='1',
                  taskLog='开始执行')
    try:
        # 程序开始时间
        startTime = time.time()
        # 创建日志保存工具类

        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='0',
                      taskLog='开始创建数据源')
        # 备份表所在的数据源
        pdbLink = "crmkf"
        # 集中创建数据源实例
        ora = Dbutil.OraclePolls(dbLinkName)
        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='0',
                      taskLog='创建数据源成功')
        # 调用实现的方法
        regionExecute()
        # 程序结束时间
        startEnd = time.time()
        exeTime = startEnd - startTime
    except func_timeout.exceptions.FunctionTimedOut as errorcode:
        logging.info("程序执行超时")
        # 数据回滚
        ora.dataRollback()
    else:
        logging.info("程序执行结束")
        logging.info("程序执行总时间为:")
        logging.info(exeTime)
        log.logInsert(jobName='boeCbossOaoJhsbZiyu', taskPos='CBOSS-OAO激活失败自动修改为DF', taskStatus='2',
                      taskLog='执行成功', doneDate=str(exeTime)[0:5] + "s")
    finally:
        ora.dataClose()
        del ora
        sys.exit(0)

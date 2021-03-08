# Time     2020/11/30 13::28
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
import sys
import math
from ProgramConfigurationOnly import timeLog
from ProgramConfigurationOnly import Config
from func_timeout import func_set_timeout
import func_timeout

staticConfig = None
logging = None
ora = None
conDict = None
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

# Time      2020/09/10 11:33:00
# Auhtor    shangxb
# Verion    1.1
# function  CIP00064数据提取。
def OrderSyncStateInsert():
    try:
        # 分批执行，按照配置表里的配置来进行
        # ------------------------begin----------------------------------------------------------------------
        # --------------

        # 统计当前符合条件的数据量有多少
        crmSelectSql1 = """
                        select count(1) as countNum
                        from devops_aigoc.order_sync_state t
                        where t.processtime >= trunc(sysdate)
                              and t.ordersyncstate = 0
                        """
        crmOrdValues = ora.selectSqlExecute("cboss", crmSelectSql1)
        countNum = int(crmOrdValues[0]["countNum"])
        logging.info("当前需要处理的数据为：" + str(countNum))

        # 判断获取数据集是否为空
        if countNum == 0:
            logging.info("没有需要修复的数据！")
            return
        # 配置表中一次扫描的数据量
        batchnum = 0
        # 分批的数量
        batchnumLis = 0

        # 读取配置表中每次扫描的数量，配置有则开启分批处理
        if "BATCHNUM" in conDict:
            try:
                batchnum = int(conDict['BATCHNUM'])
                batchnumLis = math.ceil(countNum / batchnum) if batchnum > 0 else 0
                logging.info("开启分批处理,计划分" + str(batchnumLis) + "个批次处理")
            except Exception as error:
                raise Exception("配置BATCHNUM不为整数")
        else:
            logging.info("不开启分批处理")

        # 主要程序逻辑
        for i in range(1, batchnumLis + 1):
            # 每个批次的数据单独提交
            try:
                logging.info("第" + str(i) + "批次开始执行：")
                # 根据待处理的数据量来判断分页的条件
                dictValues = {
                    "rownum": "and rownum <= " + str(batchnum) if batchnumLis > 0 else ""
                }
                crmSelectSql1 = """
                                select * from devops_aigoc.order_sync_state t
                                where t.processtime >= trunc(sysdate)
                                      and t.ordersyncstate = 0
                                      [:rownum]
                                """
                crmOrdValues = ora.selectSqlExecute("cboss", crmSelectSql1, dictValues)

                for crmOrdValue in crmOrdValues:
                    regionId = int(crmOrdValue['REGION_ID'])
                    vso, vsoDb = regionCodes(regionId)
                    regionCode = str(vso) if vso == '' else (str(vso) + ".")
                    dictValues = {
                        "regionCode": regionCode,
                        "region": str(regionId),
                        "orderId": "'" + crmOrdValue['orderid'] + "'"
                    }
                    sql = """
                          select * from [:regionCode]ord_interate_subord_sync_[:region] a
                          where order_id = [:orderId]
                          """

                    values = ora.selectSqlExecute(vsoDb, sql, dictValues)

                    if len(values) == 0:
                        continue

                    systime = str(values[0]['CREATE_DATE'])

                    dictValues = {
                        "synctime": "to_date('" + systime + "','yyyy-mm-dd hh24:mi:ss')",
                        "orderId": "'" + crmOrdValue['orderid'] + "'"
                    }
                    sql = """
                          update devops_aigoc.order_sync_state t
                          set t.synctime = [:synctime],
                              t.ordersyncstate = 1
                          where t.orderid = [:orderId]
                          """
                    ora.sqlExecute("cboss", sql, dictValues)

            except Exception as errorCode:
                logging.error(errorCode)
                logging.error("异常输出并回滚数据")
                ora.dataRollback()
                ora.dataClose()
                raise Exception(errorCode)
            else:
                ora.dataCommit()
            finally:
                logging.info("第" + str(i) + "批次结束执行！\n\n")
    except Exception as error:
        raise error


# Time    2020/09/16 11:33:00
# Author   ShangXb
# Version   1.0
# Function  循环11地市，判断每个地市对应的用户和数据源，然后根据数据源和用户调用处理的方法
#           func_set_timeout（10*60）  正常情况下数据处理时间不会超过10分钟，如果程序执行时间超过10分钟，程序自动抛出超时异常
@func_set_timeout(10 * 60)
def regionExecute():
    try:
        OrderSyncStateInsert()
    except Exception as error:
        raise error


@timeLog("主程序")
def execFunc():
    try:
        regionExecute()
    except Exception as error:
        logging.error("程序异常结束：")
        logging.error(error)
        sys.exit(1)
    else:
        logging.info("程序执行结束")
        sys.exit(0)


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    config = Config("OrderSyncStateInsert")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    pdbLink = "backTable"
    execFunc()


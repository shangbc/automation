# Time     2020/11/30 13::28
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
import sys
import math
import datetime
from ProgramConfigurationOnly import timeLog
from ProgramConfigurationOnly import Config
from func_timeout import func_set_timeout
import func_timeout

staticConfig = None
logging = None
ora = None
conDict = None


# Time      2020/09/10 11:33:00
# Auhtor    shangxb
# Verion    1.1
# function  CIP00064数据提取。
def OrderSyncStateInsert():
    try:
        month = datetime.datetime.now().strftime("%Y%m")
        dictVaule = {
            "yyyymm": month
        }
        # 统计当前符合条件的数据量有多少
        crmSelectSql1 = """
                        select count(1) as countNum
                        from cboss.home_busi_log_nk_[:yyyymm] a
                        where a.processtime > sysdate - 10 / 1440
                              and a.activitycode = 'CIP00064'
                              and SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1, 2) + 1, 2) in('21', '22', '24')
                        """
        crmOrdValues = ora.selectSqlExecute("cboss", crmSelectSql1, dictVaule)
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
                    "rownum": "and rownum <= " + str(batchnum) if batchnumLis > 0 else "",
                    "yyyymm": month
                }
                crmSelectSql1 = """
                                select a.done_code, SUBSTR(a.EXT_1, 0, INSTR(A.EXT_1, '|', 1) - 1) as orderid
                                from cboss.home_busi_log_nk_[:yyyymm] a
                                where a.processtime > sysdate - 10 / 1440
                                      and a.activitycode = 'CIP00064'
                                      and SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1, 2) + 1, 2) in('21', '22', '24')
                                      [:rownum]
                                """
                crmOrdValues = ora.selectSqlExecute("cboss", crmSelectSql1, dictValues)

                for crmOrdValue in crmOrdValues:
                    orderid = {
                        "orderid": "'" + crmOrdValue['orderid'] + "'"
                    }
                    sql = """
                          select count(*) as nullBool from devops_aigoc.order_sync_state t
                          where t.orderid = [:orderid]
                          """
                    valuse = ora.selectSqlExecute("cboss", sql, orderid)
                    nullBool = valuse[0]["nullBool"]

                    if nullBool != 0:
                        continue

                    orderid = {
                        "doneCode": str(crmOrdValue['done_code']),
                        "yyyymm": month
                    }
                    sql = """
                          insert into devops_aigoc.order_sync_state 
                          select a.activitycode,
                          b.REGION_ID,
                          SUBSTR(a.EXT_1, 0, INSTR(A.EXT_1, '|', 1) - 1) as orderid,
                          SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1)+1, INSTR(A.EXT_1, '|', 1, 2)-INSTR(A.EXT_1, '|', 1)-1) as phone,
                          SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1,2)+1, INSTR(A.EXT_1, '|', 1, 3)-INSTR(A.EXT_1, '|', 1,2)-1) as ordertype,
                          a.processtime,
                          null as synctime,
                          0 as ordersyncstate
                          from cboss.home_busi_log_nk_[:yyyymm] a, CBOSS.RES_NUMBER_HLR b
                          where a.processtime > sysdate - 10 / 1440
                                and a.activitycode = 'CIP00064'
                                AND SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1) + 1, 7) = b.NET_ID || b.HLR_SEGMENT
                                and SUBSTR(a.EXT_1, INSTR(A.EXT_1, '|', 1, 2) + 1, 2) in ('21', '22', '24')
                                and a.done_code = [:doneCode]
                          """
                    ora.sqlExecute("cboss", sql, orderid)

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

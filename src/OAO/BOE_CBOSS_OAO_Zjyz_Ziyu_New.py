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


# Time      2020/09/10 11:33:00
# Auhtor    shangxb
# Verion    1.1
# function  OAO报证件被预占导致激活失败，等待30分钟重新激活。
def boeCbossOaoJZjyuZjyu(vso, vsoDb, region):
    try:
        # select * from sox.ord_interate_subord_sync_xxx a
        # where done_date >trunc(sysdate)
        #       and done_date<sysdate-35/1440
        #       and error_msg='H04-激活不成功,证件号码被预占'
        #       and state='AF'
        #       and (n_ext_1 is null or  n_ext_1!=0)
        # 分批执行，按照配置表里的配置来进行
        # ------------------------begin----------------------------------------------------------------------
        # --------------

        # 判断是否需要输入用户名
        regionCode = str(vso) if vso == '' else (str(vso) + ".")
        repairNum = 0

        # 统计当前符合条件的数据量有多少
        dictValues = {
            "regionCode": regionCode,
            "region": str(region)
        }
        crmSelectSql1 = """
                        select count(*) as countNum from [:regionCode]ord_interate_subord_sync_[:region] a
                        where done_date > trunc(sysdate)
                        and done_date < sysdate-35/1440
                        and error_msg = 'H04-激活不成功,证件号码被预占'
                        and state in ('AF','01')
                        and (n_ext_1 is null or  n_ext_1 != 0)
                        """
        crmOrdValues = ora.selectSqlExecute(vsoDb, crmSelectSql1, dictValues)
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
                    "regionCode": regionCode,
                    "region": str(region),
                    "rownum": "and rownum <= " + str(batchnum) if batchnumLis > 0 else ""
                }
                crmSelectSql1 = """
                                select * from [:regionCode]ord_interate_subord_sync_[:region] a
                                where done_date > trunc(sysdate)
                                and done_date < sysdate-35/1440
                                and error_msg = 'H04-激活不成功,证件号码被预占'
                                and state in ('AF','01')
                                and (n_ext_1 is null or  n_ext_1 != 0)
                                [:rownum]
                                """
                crmOrdValues = ora.selectSqlExecute(vsoDb, crmSelectSql1, dictValues)
                orderId = "'" + "','".join([crmOrdValue['ORDER_ID'] for crmOrdValue in crmOrdValues]) + "'"
                dictValues = {
                    "regionCode": regionCode,
                    "region": str(region),
                    "orderId": "(" + orderId + ")"
                }
                updateSqlExt1Null = """
                                    update [:regionCode]ord_interate_subord_sync_[:region] a
                                    set a.state = 'DF',
                                        a.active_flag = '2',
                                        a.n_ext_1 = 0
                                    where done_date > trunc(sysdate)
                                          and done_date < sysdate-35/1440
                                          and error_msg = 'H04-激活不成功,证件号码被预占'
                                          and state in ('AF','01')
                                          and (n_ext_1 is null or  n_ext_1 != 0)
                                          and order_id in [:orderId]
                                    """
                ora.sqlExecute(vsoDb, updateSqlExt1Null, dictValues)
                logging.info("数据修复成功！")

                # 备份数据不应该影响数据的修复，所以捕获异常
                # 数据备份
                # -------------------------------begin-----------------------------------------------------------
                try:
                    # 传入表名和数据集，方法会返回insert语句和处理后的数据集
                    # 执行批量插入
                    ora.batchInsertAll(pdbLink, "bomc.ord_interate_subord_sync_zy", crmOrdValues)
                except Exception as error:
                    logging.error("数据备份到bomc.ord_interate_subord_sync_zy失败，不影响数据的修复！")
                else:
                    logging.info("数据备份到bomc.ord_interate_subord_sync_zy成功！")
                # --------------------------------end------------------------------------------------------------

            except Exception as errorCode:
                logging.error(errorCode)
                logging.error("异常输出并回滚数据")
                ora.dataRollback()
                ora.dataClose()
                raise Exception(errorCode)
            else:
                ora.dataCommit()
                logging.info("本批次修复的数据为：")
                logging.info(orderId)
                repairNum += len(crmOrdValues)
            finally:
                logging.info("第" + str(i) + "批次结束执行！\n\n")
    except Exception as error:
        raise error
    finally:
        if repairNum != 0:
            try:
                # 记录相关的治愈信息
                dbpRptInsertSql = "insert into bomc.RPT_GW_AUTOFIX values ('CBOSS-OAO证件预占自动修改为DF','陈斌',trunc(sysdate)," + \
                                  str(repairNum) + ",sysdate)"
                ora.sqlExecute(pdbLink, dbpRptInsertSql)
            except Exception as error:
                logging.error("修复记录失败，不影响数据修复！！")
                raise error
            else:
                logging.info("修复数入表成功！")
                ora.dataCommit()


# Time    2020/09/16 11:33:00
# Author   ShangXb
# Version   1.0
# Function  循环11地市，判断每个地市对应的用户和数据源，然后根据数据源和用户调用处理的方法
#           func_set_timeout（10*60）  正常情况下数据处理时间不会超过10分钟，如果程序执行时间超过10分钟，程序自动抛出超时异常
@func_set_timeout(10 * 60)
def regionExecute():
    # 地市
    region = None
    # 存放用户
    vso = None
    # 存放数据源名
    vsoDb = None
    try:
        if "REGION" in conDict:
            region = int(conDict['REGION'])
        else:
            raise Exception("没有配置对应的REGION！！！")
        if "TABUSER" in conDict:
            vso = conDict['TABUSER']
        else:
            raise Exception("没有配置对应的TABUSER！！！")
        if "TABDB" in conDict:
            vsoDb = conDict['TABDB']
        else:
            raise Exception("没有配置对应的TABDB！！！")
        boeCbossOaoJZjyuZjyu(vso, vsoDb, region)
    except Exception as error:
        raise error
    finally:
        del vso, vsoDb


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
    config = Config("BOE_CBOSS_OAO_ZJYU_ZJYU_579")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    pdbLink = "backTable"
    execFunc()

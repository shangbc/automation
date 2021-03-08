# Time     2021/01/13 15::29
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config
import datetime


#
def OaoFeedback():
    try:
        sql = """
              select 'CIP00065预处理反馈' as INTERFACE_CODE,
                     '积压量' as INDEX_CODE,
                     '' as LINK_NAME,
                     n.city_code as CITY_CODE,
                     decode(m.index_vaule,null,0,m.index_vaule) as INDEX_VAULE,
                     0 as SUCCESS_NUMS,
                     0 as BUSINESS_FAIL_NUMS,
                     trunc(sysdate - 5 / 1440,'mi') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from bomc.cboss_city_code n
              left join (select t.region_id, count(1) index_vaule
                         from bomc.oao_feedback_state t
                         where t.order_state = 'PC'
                               and t.feedback_state = 0
                               and t.state = 'Y'
                         group by t.region_id) m
              on n.city_code = m.region_id
              """
        yield ora.selectSqlExecute("backTable", sql)

        sql = """
                  select 'CIP00065' as INTERFACE_CODE,
                         'BACKLOG_COUNT' as INDEX_CODE,
                         'AC_AF_STATE' as LINK_NAME,
                         n.city_code as CITY_CODE,
                         decode(m.index_vaule,null,0,m.index_vaule) as INDEX_VAULE,
                         0 as SUCCESS_NUMS,
                         0 as BUSINESS_FAIL_NUMS,
                         trunc(sysdate - 5 / 1440,'mi') as START_TIME,
                         trunc(sysdate,'mi') as END_TIME,
                         sysdate as CREATE_TIME
                  from bomc.cboss_city_code n
                  left join (select t.region_id, count(1) index_vaule
                             from bomc.oao_feedback_state t
                             where t.order_state in ('AC', 'AF')
                                   and t.feedback_state = 0
                                   and t.state = 'Y'
                             group by t.region_id) m
                  on n.city_code = m.region_id
                  """
        yield ora.selectSqlExecute("backTable", sql)

        sql = """         
              select 'CIP00065' as INTERFACE_CODE,
                     'BACKLOG_COUNT' as INDEX_CODE,
                     'AC_AF_STATE' as LINK_NAME,
                     'ALL' as CITY_CODE,
                     decode(m.index_vaule,null,0,m.index_vaule) as INDEX_VAULE,
                     0 as SUCCESS_NUMS,
                     0 as BUSINESS_FAIL_NUMS,
                     trunc(sysdate - 5 / 1440,'mi') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from (select count(1) index_vaule
                    from bomc.oao_feedback_state t
                    where t.order_state in ('AC','AF')
                          and t.feedback_state = 0
                          and t.state = 'Y') m
              """
        yield ora.selectSqlExecute("backTable", sql)

        sql = """         
              select 'CIP00065' as INTERFACE_CODE,
                     'BACKLOG_COUNT' as INDEX_CODE,
                     'PC_STATE' as LINK_NAME,
                     'ALL' as CITY_CODE,
                     decode(m.index_vaule,null,0,m.index_vaule) as INDEX_VAULE,
                     0 as SUCCESS_NUMS,
                     0 as BUSINESS_FAIL_NUMS,
                     trunc(sysdate - 5 / 1440,'mi') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from (select count(1) index_vaule
                    from bomc.oao_feedback_state t
                    where t.order_state = 'PC'
                          and t.feedback_state = 0
                          and t.state = 'Y') m
              """
        yield ora.selectSqlExecute("backTable", sql)

    except Exception as error:
        logging.error(error)


def OaoStateData(user, vsoDb, region):

    try:
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")
        dictVaule = {
            "citycCode": str(region),
            "user": user
        }
        sql = """
              select 'JY_OAO_PRE_OPEN' as INTERFACE_CODE,
                     'BACKLOG_COUNT' as INDEX_CODE,
                     'ET' as LINK_NAME,
                     '[:citycCode]' as CITY_CODE,
                     count(1) as  INDEX_VAULE,
                     trunc(sysdate - 30,'mi') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from [:user]ord_interate_subord_sync_[:citycCode] t
              where t.create_date >= trunc(sysdate - 30)
                    and t.done_date < sysdate - 2/1440
                    and t.state in ('PF','DN')
                    and t.number_opr_type in ('20', '21', '23')
              """
        vauleList = ora.selectSqlExecute(vsoDb, sql, dictVaule)
        yield vauleList

        sql = """
              select 'JY_OAO_OPEN' as INTERFACE_CODE,
                     'BACKLOG_COUNT' as INDEX_CODE,
                     'ET' as LINK_NAME,
                     '[:citycCode]' as CITY_CODE,
                     count(1) as  INDEX_VAULE,
                     trunc(sysdate,'hh') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from [:user]ord_interate_subord_sync_[:citycCode] t
              where t.done_date >= trunc(sysdate)
                    and t.done_date < sysdate - 2/1440
                    and t.state in ('DF','AF')
                    and t.number_opr_type in ('20', '21', '23')    
              """
        vauleList = ora.selectSqlExecute(vsoDb, sql, dictVaule)

        yield vauleList

        sql = """
              select 'JY_OAO_OPEN' as INTERFACE_CODE,
                     'AC_COUNT' as INDEX_CODE,
                     'ET' as LINK_NAME,
                     '[:citycCode]' as CITY_CODE,
                     count(1) as INDEX_VAULE,
                     trunc(sysdate,'hh') as START_TIME,
                     trunc(sysdate,'mi') as END_TIME,
                     sysdate as CREATE_TIME
              from [:user]ord_interate_subord_sync_[:citycCode] t
              where t.done_date >= trunc(sysdate)
                    and t.done_date < sysdate - 2/1440
                    and t.state = 'AC'
                    and t.number_opr_type in ('20', '21', '23')  
              """
        vauleList = ora.selectSqlExecute(vsoDb, sql, dictVaule)

        yield vauleList
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
        yield from OaoStateData(user, vsoDb, region)
    del vso, vsoDb


def execFunc():
    table = "bomc.cboss_interface_data_" + datetime.datetime.now().strftime("%Y%m")
    for func in regionExecute():
        try:
            if len(func) == 0:
                return
            sql, dataList = ora.batchInsertSql(table, func, "backTable")
            logging.info(sql)
            logging.info(dataList)
            ora.batchInsert("backTable", sql, dataList)
        except Exception as error:
            logging.error(error)
            ora.dataRollback()
        else:
            ora.dataCommit()

    for func in OaoFeedback():
        try:
            if len(func) == 0:
                return
            sql, dataList = ora.batchInsertSql(table, func, "backTable")
            logging.info(sql)
            logging.info(dataList)
            ora.batchInsert("backTable", sql, dataList)
        except Exception as error:
            logging.error(error)
            ora.dataRollback()
        else:
            ora.dataCommit()


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    config = Config("OrderSyncStateInsert")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    pdbLink = "backTable"
    execFunc()

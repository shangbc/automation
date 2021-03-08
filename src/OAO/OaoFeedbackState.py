# Time     2021/01/18 12::42
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 获取应该通过CIP00065接口返回的订单状态信息
from ProgramConfigurationOnly import Config


def OaoFeedbackState(user, vsoDb, region):
    try:
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")
        dicValue = {
            "user": user,
            "region": str(region),
        }
        # 查询为反馈的订单数据
        selectSql = """
                    select t.order_id "ORDER_ID",
                           t.state as "ORDER_STATE",
                           sysdate as "CREATE_DATE",
                           t.region_id "REGION_ID",
                           0 as "FEEDBACK_STATE",
                           'Y' as "STATE"
                    from [:user]ord_interate_subord_sync_[:region] t
                    where t.state in ('PC', 'AC', 'AF', 'FA')
                          and t.error_msg <> 'H17-业务规则限制无法办理-物流商反馈'
                          and t.error_msg <> 'H01-地址校验失败'
                          and t.number_opr_type in ('20', '21', '23')
                          and t.done_date >= trunc(sysdate)
                          and t.done_date < sysdate - 10 / 1440
                    """
        orderList = ora.selectSqlExecute(vsoDb, selectSql, dicValue)
        yield orderList
    except Exception as error:
        logging.error(error)
    finally:
        logging.info(str(region) + "地市执行结束！")


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
        yield from OaoFeedbackState(user, vsoDb, region)
    del vso, vsoDb


def Exefunc(regionExecuteFunc):
    for fun in regionExecuteFunc:
        insertList = fun

        if len(insertList) == 0:
            return

        for insert in insertList[::-1]:
            dictInsert = {
                "order_id": insert['order_id'],
                "state": insert['order_state']
            }
            sql = """
                  select count(1) as nums from bomc.oao_feedback_state t
                  where t.order_id = '[:order_id]'
                        and t.order_state = '[:state]'
                  """
            vaule = ora.selectSqlExecute("pd", sql, dictInsert)
            if int(vaule[0]['nums']) > 0:
                insertList.remove(insert)

        try:
            sql, vauleList = ora.batchInsertSql("bomc.oao_feedback_state", insertList, "pd")
            ora.batchInsert("pd", sql, vauleList)
        except Exception as error:
            ora.dataRollback()
        else:
            ora.dataCommit()
        finally:
            ora.dataClose()


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    config = Config("OAOFeedback")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    regionExecute = regionExecute()
    Exefunc(regionExecute)

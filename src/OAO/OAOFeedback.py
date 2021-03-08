# Time     2021/01/04 10::29
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function OAO修复失败反馈
from ProgramConfigurationOnly import Config
import json


def OAOFeedback(user, vsoDb, region):
    try:
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")

        # 匹配条件
        like = "1 = 1 "
        # 读取配置中的匹配条件
        if "LIKE" not in conDict:
            like = "t.error_msg like 'H04-激活不成功,实名制规则校验不通过%'"
        else:
            likeStr = conDict['LIKE']
            likeDictList = eval(likeStr)
            for likeDict in likeDictList:
                likeSql = "or t.error_msg" + (" like " if likeDict['key'] == 'like' else " = ")
                likeSql = likeSql + "'" + likeDict['vaule'] + "' "
                like += likeSql
            like = "(" + like + ")"

        dicValue = {
            "user": user,
            "region": str(region),
            "like": like
        }

        # 查询为反馈的订单数据
        selectSql = """
                    select * from [:user]ord_interate_subord_sync_[:region] t
                    where t.state = '01'
                          and t.n_ext_1 = 9
                          and [:like]
                          and t.done_date >= trunc(sysdate) - 3 
                          and t.done_date < sysdate - 10/24
                    """
        # 获取需要修改的数据
        vauleList = ora.selectSqlExecute(vsoDb, selectSql, dicValue)
        if len(vauleList) == 0:
            logging.info("当前没有需要修复的数据！")
            return

        # 取数指定的字段
        orderIdList = [{"state": "03", "orderId": vaule['order_id']} for vaule in vauleList]
        updateSql = ""
        # 状态修改为03
        if region != 572:
            updateSql = """
                        update [:user]ord_interate_subord_sync_[:region] t
                        set t.state = :state
                        where t.order_id = :orderId
                        """
        else:
            updateSql = """
                        update [:user]ord_interate_subord_sync_[:region] t
                        set t.state = %(state)s
                        where t.order_id = %(orderId)s
                        """
        updateSql = ora.strFormat(updateSql, dicValue)
        ora.batchExec(vsoDb, updateSql, orderIdList)
    except Exception as error:
        logging.error(error)
        ora.dataRollback()
    else:
        # 数据提交
        ora.dataCommit()
    finally:
        logging.info(str(region)+"地市执行结束！")


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
        OAOFeedback(user, vsoDb, region)
    del vso, vsoDb


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    config = Config("OAOFeedback")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    regionExecute()

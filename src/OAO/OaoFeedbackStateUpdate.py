# Time     2021/01/18 12::42
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 获取应该通过CIP00065接口返回的订单状态信息
from ProgramConfigurationOnly import Config
import datetime
import sys


def OaoFeedbackStateUpdate():
    try:
        sql = """
             select * from bomc.oao_feedback_state t
             where t.state = 'Y'
             """
        vauleList = ora.selectSqlExecute("pd", sql)

        if len(vauleList) == 0:
            return []

        return vauleList

    except Exception as error:
        raise error


def update(date):
    try:
        vauleDict = {
            "order_id": date['order_id'],
            "order_state": date['order_state']
        }

        sql = """
              update  bomc.oao_feedback_state t
              set t.feedback_state = 1,t.state = 'N', t.done_date = sysdate
              where t.order_id = '[:order_id]'
                    and t.order_state = '[:order_state]'
              """
        ora.sqlExecute("pd", sql, vauleDict)
    except Exception as error:
        logging.error(error)
        ora.dataRollback()
    else:
        ora.dataCommit()


def dateUpdate(dataList):
    now_month = datetime.datetime.now().strftime("%Y%m")
    last_month = datetime.datetime.now().strftime("%Y%m")
    for data in dataList:
        dictVaule = {
            "now_month": now_month,
            "order_id": data['order_id'],
            "order_state": data['order_state']
        }
        sql = """
              select * from party.aop_records_log_[:now_month] t
              where t.order_id = '[:order_id]'
                    and t.order_state = '[:order_state]'
              """
        vaule = ora.selectSqlExecute("res", sql, dictVaule)
        if len(vaule) > 0:
            update(data)
            continue

        dictVaule = {
            "now_month": last_month,
            "order_id": data['order_id'],
            "order_state": data['order_state']
        }
        sql = """
              select * from party.aop_records_log_[:now_month] t
              where t.order_id = '[:order_id]'
                    and t.order_state = '[:order_state]'
              """
        vaule = ora.selectSqlExecute("res", sql, dictVaule)
        if len(vaule) > 0:
            update(data)


if __name__ == '__main__':

    try:
        # 程序配置在cboss.python_config_cboss表中
        config = Config("OAOFeedback")
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()
        updateList = OaoFeedbackStateUpdate()
        dateUpdate(updateList)
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        sys.exit(0)

# Time     2021/01/12 14::52
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config
import datetime
import sys
import json


# 执行表中的取数sql  单语句
def getInterfaceData(sql, users):
    try:
        # 获取当月月份
        month = datetime.datetime.now().strftime("%Y%m")
        sql = str(sql)

        # 拼接sql
        dictVaule = {
            "yyyymm": month,
        }

        # 执行查询
        data = ora.selectSqlExecute(users, sql, dictVaule)
        yield data
    except BaseException as error:
        logging.error(error)


# 执行表中的取数sql  地市循环
def getInterfaceDatas(user, vsoDb, region, sql):
    try:
        # 判断是否需要输入用户名
        user = str(user) if user == '' else (str(user) + ".")
        sql = str(sql)
        # 拼装条件
        dictVaule = {
            "citycCode": str(region),
            "user": user
        }

        # 返回结果
        vauleList = ora.selectSqlExecute(vsoDb, sql, dictVaule)
        yield vauleList
    except Exception as error:
        logging.error(error)


def regionExecute(sql, loop):
    try:
        # 数据转换
        loop = ora.strFormat(str(loop))
        loopDict = eval(str(loop))

        # 存放用户
        vso = None
        # 存放数据源名
        vsoDb = None

        # 循环11个地市
        for region in range(570, 581):

            # 根据地市编号返回用户名和数据库连接名
            user = loopDict[str(region)]['user']
            vsoDb = loopDict[str(region)]['dbname']
            yield from getInterfaceDatas(user, vsoDb, region, sql)
    except Exception as error:
        raise error
    finally:
        del vso, vsoDb


# 获取配置表的数据
def execFunc():
    try:
        sql = """
              select * from bomc.cboss_index_sql t
              where t.state = 1
              """
        indexCodes = ora.selectSqlExecute("backTable", sql)

        # 配置表中存放指标的取数sql
        if len(indexCodes) == 0:
            raise Exception("配置表【cboss_index_sql】中没有配置指定的sql")

        # 循环配置表的各个sql
        for indexCode in indexCodes:
            if int(indexCode['loop']) == 0:
                # 接收查询的数据并返回 单语句
                yield from getInterfaceData(indexCode['index_sql'], indexCode['users'])
            else:
                yield from regionExecute(indexCode['index_sql'], indexCode['loop_users'])

    except Exception as error:
        raise error


# 数据入库
def func(execFunc):
    try:
        insertList = []
        for fun in execFunc:
            insertList.extend(fun)
        month = datetime.datetime.now().strftime("%Y%m")
        table = "bomc.cboss_interface_data_" + month
        sql, dataList = ora.batchInsertSql(table, insertList, "backTable")
        ora.batchInsert("backTable", sql, dataList)
    except Exception as error:
        ora.dataRollback()
        raise error
    else:
        ora.dataCommit()


if __name__ == '__main__':
    try:
        # 程序配置在cboss.python_config_cboss表中
        config = Config("AccountCancellationSearch")
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()
        execFunc = execFunc()
        func(execFunc)
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        sys.exit(0)

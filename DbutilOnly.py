import numpy as np
from NewModules_public import OraclePool, DadbPool
import pandas as pd
import datetime
from pandas._libs.tslibs.timestamps import Timestamp
from pandas._libs.tslibs.nattype import NaTType, NaT
import time
import sys
import func_timeout
import re
from func_timeout import func_set_timeout


# @Time    2020/09/15 11:33:00
# Auhtor   shangxb
# Verion   1.0
# function 数据源管理类，方便数据源的集中管理和sql执行失败抛出对应异常
#          由于NewModules在实现时进行了内部的异常捕获，导致外层无法捕获到相关的异常，所以在本层执行时对结果进行了相关的判断和异常抛出，
#          便于外层事务回滚的实现
#          测试专用工具类

# 日志输出控制
def loggingDisplay(func):
    def wrap(self, *args, **kwargs):
        self.logging.display()
        fun = func(self, *args, **kwargs)
        self.logging.hide()
        return fun

    return wrap


# 批量操作之前对sql和数据进行处理
def batchExecSqlFromat(frun):
    def wrap(self, dbName, v_sql, list, dictVaule=None):
        v_sql = self.strFormat(v_sql, dictVaule)
        if self.dbTypeMap.get(dbName) != '0':
            occupierList = re.findall(r':\w+', v_sql)
            for occupier in occupierList:
                string = "%(" + occupier + ")s"
                v_sql = v_sql.replace(occupier, string)
        return frun(self, dbName, v_sql, list, dictVaule)

    return wrap


class OraclePolls:

    # 初始化数据源实例
    def __init__(self, dbNameList, logging):
        try:
            self.dbNameMap = {}
            self.dbTypeMap = {}
            dbNameType = type(dbNameList)
            if dbNameType is not list:
                raise Exception("传入的值不为字典列表")
            for dbNames in dbNameList:
                try:
                    dbLink = None
                    userName = dbNames["userName"]
                    linkName = dbNames["linkName"]
                    state = dbNames["state"]
                    # state 0是采用Oracle创建,1是采用Dadb创建
                    if state == '0':
                        dbLink = OraclePool(linkName)
                    else:
                        dbLink = DadbPool(linkName)
                    # 判断获取数据源是否成功
                    if dbLink is None:
                        raise Exception("数据源" + userName + "创建失败")
                    logging.debug("数据源" + userName + "创建成功")
                    self.dbNameMap.setdefault(userName, dbLink)
                    self.dbTypeMap.setdefault(userName, state)
                except Exception as errorCode:
                    if "list index out of range" == str(errorCode):
                        raise ("数据库连接名{}在配置表中查询不到！".format(linkName))
                    raise errorCode
        except Exception as errorCode:
            raise errorCode
            sys.exit(1)
        else:
            self.logging = logging
            self.logging.info("数据源加载成功")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 根据传入的函数对list内容进行整体修改
    def updateArrayList(self, arrayList, function):
        return [function(array) for array in arrayList]

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 根据定义的数据源名获取数据源实例
    def getDbName(self, dbName):
        dbNameBool = dbName in self.dbNameMap
        # 根据key值获取数据源，存在时返回对应的实例，否则抛出异常
        if dbNameBool is True:
            return self.dbNameMap.get(dbName)
        else:
            raise Exception("根据" + dbName + "无法获取对应的数据源实例")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 添加一个新的数据源
    def addDbname(self, dbName, dbValue):
        try:
            dbNameBool = dbName in self.dbNameMap
            if dbNameBool is False:
                dbLink = OraclePool(dbValue)
                self.dbNameMap.setdefault(dbName, dbLink)
        except Exception as errorCode:
            self.logging.error(errorCode)
        else:
            self.logging.info("添加数据源" + dbValue + "成功")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 查询，判断查询失败时手动抛出异常
    @loggingDisplay
    def selectSqlExecute(self, dbName, sql, dictVaule=None, *parameterList):
        try:
            if dbName in self.dbNameMap:
                dbLink = self.dbNameMap.get(dbName)
                sql = self.strFormat(sql, dictVaule=dictVaule)

                if len(parameterList) == 0:
                    data = dbLink.select(sql)
                else:
                    data = dbLink.select(sql, *parameterList)
                if data is False:
                    raise Exception("sql执行异常")
                return data
            else:
                raise Exception("数据源不存在")
        except Exception as error:
            raise error

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function dll语句执行，判断执行失败时手动抛出异常
    def sqlExecute(self, dbName, sql, dictVaule=None):
        if dbName in self.dbNameMap:
            dbLink = self.dbNameMap.get(dbName)
            sql = self.strFormat(sql, dictVaule=dictVaule)
            code = dbLink.execute_sql(sql)
            if code is False:
                raise Exception("执行失败")
        else:
            raise Exception("数据源不存在")

        # @Time    2020/09/15 11:33:00
        # Auhtor   shangxb
        # Verion   1.0
        # function dll语句执行，判断执行失败时手动抛出异常

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 批量插入数据，减少与数据库交互的次数
    @loggingDisplay
    def batchInsert(self, dbName, v_sql, list):
        if dbName in self.dbNameMap:
            dbLink = self.dbNameMap.get(dbName)
            # # 以防数据源没有创建导致插入失败
            # sql = "select * from dual"
            # dbLink.select(sql)
            # connCursor = dbLink.cursor
            try:
                code = dbLink.execute_sql_many(v_sql, list)
            except Exception as errorcode:
                self.logging.info("批量插入执行失败")
                self.logging.error(errorcode)
                raise Exception(errorcode)
            else:
                self.logging.info("批量插入执行完成")
                return code
        else:
            raise Exception("数据源不存在")

    @loggingDisplay
    @batchExecSqlFromat
    def batchExec(self, dbName, v_sql, list, dictVaule=None):
        if dbName in self.dbNameMap:
            dbLink = self.dbNameMap.get(dbName)
            # # 以防数据源没有创建导致插入失败
            # sql = "select * from dual"
            # dbLink.select(sql)
            # connCursor = dbLink.cursor
            try:
                code = dbLink.execute_sql_many(v_sql, list)
            except Exception as errorcode:
                self.logging.info("批量执行失败")
                self.logging.error(errorcode)
                raise Exception(errorcode)
            else:
                self.logging.info("批量执行成功")
                return code
        else:
            raise Exception("数据源不存在")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 批量插入数据，减少与数据库交互的次数
    #          tableName为表名，valuesList为数据列表
    def batchInsertAll(self, dbName, tableName, valuesList):
        if dbName in self.dbNameMap:
            dbLink = self.dbNameMap.get(dbName)
            # 以防数据源没有创建导致插入失败
            sql = "select * from dual"
            dbLink.select(sql)
            connCursor = dbLink.cursor
            try:
                v_sql, list = self.batchInsertSql(tableName, valuesList, dbName)
                code = connCursor.executemany(v_sql, list)
            except Exception as errorcode:
                self.logging.error("批量插入执行失败")
                self.logging.error(errorcode)
                raise Exception(errorcode)
            else:
                self.logging.info("批量插入执行完成")
                return code
        else:
            raise Exception("数据源不存在")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 程序或执行的语句正常完成时，对数据进行保存
    def dataCommit(self):
        try:
            for dbNamme, dbValue in self.dbNameMap.items():
                dbValue.commit()
        except Exception as errorCode:
            self.logging.error("数据提交失败: ")
            self.logging.info(errorCode)
        else:
            self.logging.info("数据提交成功")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 程序或执行的语句发生错误时，对对数据库进行数据回滚
    @loggingDisplay
    def dataRollback(self):
        try:
            for dbValue in self.dbNameMap.values():
                # 数据进行回滚
                dbValue.rollback()
        except Exception as errorCode:
            self.logging.info("数据回滚失败: ")
            self.logging.info(errorCode)
        else:
            self.logging.info("数据回滚成功")

    # @Time    2020/09/15 11:33:00
    # Auhtor   shangxb
    # Verion   1.0
    # function 程序执行结束时关闭所有资源（将链接放回连接池）
    def dataClose(self):
        try:
            for dbNamme, dbValue in self.dbNameMap.items():
                # 关闭数据源链接
                dbValue.close()
        except Exception as errorCode:
            self.logging.info("数据源连接关闭失败: ")
            self.logging.info(errorCode)
        else:
            self.logging.info("数据源连接关闭成功")

    # @Time     2020/09/15 11:33:00
    # Auhtor    shangxb
    # Verion    1.1.2
    # function  拼装批量插入所需要的insert和数据集
    #           由于查询sql返回的数据集中已经将Noen的字段剔除，所以想使用批量插入的功能必须先将表的字段补齐，同时按字段拼接对应的数据集
    # update    2020/10/09 13:36:00     新增Nan数值判断
    # update    2020/10/19 12:36:00     新增oracle和mysql预占判断参数   0为oracle   1为mysql
    def batchInsertSql(self, tableName, valuesList, dbName=None):
        # 默认采用Oracle
        state = '0'
        # 判断数据源是否存在
        columnNameList = valuesList[0]
        # 字段名列表
        nameList = ""
        # 占位符列表
        vauleList = ""
        # print(self.dbTypeMap)
        if dbName is not None and dbName not in self.dbNameMap:
            self.logging.info(dbName, "不存在")
            raise Exception("数据源不存在！")
        if dbName is not None:
            state = self.dbTypeMap.get(dbName)

        df = pd.DataFrame(valuesList)
        columnList = df.columns.values.tolist()
        # 将所有空值替换为None
        df = df.astype(object).where(pd.notnull(df), None)
        # 重新转成diict
        df = df.to_dict('records')
        nameList = ",".join(columnList)
        # 统一用字段名作为预占符
        # state  0  采用Oracle的预占方式  1采用mysql的预占方式
        if state == '0':
            vauleList = ":" + ",:".join(columnList)
        else:
            vauleList += "%(" + ")s,%(".join(columnList) + ")s"
        # 拼接sql
        dbpInsertSql = "insert into " + tableName + "(" + nameList + ") values(" + vauleList + ")"
        keyList = df
        return dbpInsertSql, keyList

    # @Time     2020/09/15 11:33:00
    # Auhtor    shangxb
    # Verion    1.1.2
    # function
    #
    def strFormat(self, string, dictVaule=None, func=lambda string: "[:" + string + "]"):
        string = string.expandtabs(tabsize=8)
        string = re.sub(r'[\s]+', ' ', string)
        string = string.strip()
        string = string + "\n"
        if dictVaule is not None:
            for key, vuale in dictVaule.items():
                strRe = func(key)
                string = string.replace(strRe, vuale)
        return string

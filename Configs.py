# Time     2020/12/03 15::02
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import StaticConfigClass
import LoggingClass
import DbutilOnly
from Decorator import timeLog


class Config:

    @timeLog("配置工具类")
    def __init__(self, pythonCode):
        try:
            # 加载数据库配置
            staticConfig = StaticConfigClass.StaticConfig(pythonCode)
            self.__staticConfig = staticConfig
            configDict = staticConfig.getKeyDict()
            # 加载日志数据配置
            logging = LoggingClass.Logging(configDict)
            self.__logging = logging
            # 加载数据源
            dataSourceConfigStr = None
            if "DATASOURCE" not in configDict:
                dataSourceConfigStr = "[{'userName': 'cboss', 'linkName': 'zjcboss', 'state': '0'}]"
                logging.info("数据源DATASOURCE没有配置，采用默认数据源zjcboss")
            else:
                dataSourceConfigStr = configDict['DATASOURCE']
            dataSourceConfig = eval(dataSourceConfigStr)
            dataSource = DbutilOnly.OraclePolls(dataSourceConfig, self.__logging)
            self.__dataSource = dataSource
        except IndexError as error:
            raise error
        except Exception as error:
            raise Exception("配置工具类加载失败！！")
        else:
            self.__logging.info("配置工具类加载成功")

    def getDataSource(self):
        return self.__dataSource

    def getLogging(self):
        return self.__logging

    def getStaticConfig(self):
        return self.__staticConfig

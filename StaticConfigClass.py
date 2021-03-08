# Time     2020/11/30 17::26
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import logging


from NewModules_public import OraclePool


class StaticConfig:
    def __init__(self, pythonCode):
        oracle = "zjcboss"
        sql = "select * from cboss.python_config_cboss t where t.python_code = 'PythonCode' and t.config_state = 'U'"
        sql = sql.replace("PythonCode", pythonCode)
        try:
            oracle = OraclePool(oracle)
            self.loggingDisplay(logging.CRITICAL)
            vauleList = oracle.select(sql)
            self.loggingDisplay(logging.INFO)
            if len(vauleList) == 0:
                raise Exception("静态配置为空，请检查{}相关的配置".format(pythonCode))
            keyDict = {}
            for vaule in vauleList:
                # logging.info(vaule['CONFIG_CODE'] + " - " + vaule['CONFIG_VAULE'] + "[" + vaule['EXPLAIN'] + "]")
                keyDict[vaule['config_code']] = vaule['config_vaule']
            self.__keyDicts = keyDict
        except Exception as error:
            logging.error("加载数据库静态配置成功" + error)
        else:
            logging.info("加载数据库静态配置成功")

    def getKeyDict(self):
        return self.__keyDicts

    def loggingDisplay(self, loggingLeve):
        strFormat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
        logging.basicConfig(format=strFormat, level=loggingLeve)



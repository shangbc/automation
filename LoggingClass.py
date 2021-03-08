# Time     2020/11/30 17::10
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import logging


# 重新设置日志级别
# DEBUG > INFO > WARNING > ERROR > CRITICAL > FATAL
def resetLogLevel(self):
    level = self.loggLevel[self.logging_level]
    strFormat = self.logging_str_format
    root_logger = logging.getLogger()
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    logging.basicConfig(format=strFormat, level=level)


# 日志输出时调用
def resetLogLevelBegin(leve):
    leveDict = {
        "DEBUG": 6,
        "INFO": 5,
        "WARNING": 4,
        "ERROR": 3,
        "CRITICAL": 2,
        "FATAL": 1
    }

    def resetLog(fun):
        def wrap(self, string):
            if leveDict[self.logging_level] < leveDict[leve]:
                return
            resetLogLevel(self)
            result = fun(self, string)
            return result

        return wrap

    return resetLog


class Logging:

    def __init__(self, keyDict):
        try:
            # 日志输出格式
            if "LOGGING_STR_FORMAT" in keyDict:
                self.logging_str_format = keyDict['LOGGING_STR_FORMAT']
            else:
                self.logging_str_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
            # 日志级别
            if "LOGGINGLEVEL" in keyDict:
                self.logging_level = keyDict['LOGGINGLEVEL']
            else:
                self.logging_level = "INFO"
            # 底层查询是否显示/隐藏
            if "LOGGINGDISPLAY" in keyDict:
                self.logging_dispaly = keyDict['LOGGINGDISPLAY']
            else:
                self.logging_dispaly = "DISPLAY"
            # 配置级别对应的输出方式
            self.loggLevel = {
                "DEBUG": logging.INFO,
                "INFO": logging.INFO,
                "WARNING": logging.WARNING,
                "ERROR": logging.ERROR,
                "CRITICAL": logging.CRITICAL,
                "FATAL": logging.FATAL
            }

        except Exception as error:
            errStr = "日志输出工具类异常：{}".format(error)
            raise Exception(errStr)
        else:
            logStr = "日志输出工具加载成功!"
            self.info(logStr)

    # DEBUG
    @resetLogLevelBegin("DEBUG")
    def debug(self, string):
        logging.debug(string)

    # info
    @resetLogLevelBegin("INFO")
    def info(self, string):
        logging.info(string)

    # WARNING
    @resetLogLevelBegin("WARNING")
    def warning(self, string):
        logging.warning(string)

    # error
    @resetLogLevelBegin("ERROR")
    def error(self, string):
        logging.error(string)

    # CRITICAL
    @resetLogLevelBegin("CRITICAL")
    def critical(self, string):
        logging.critical(string)

    # FATAL
    @resetLogLevelBegin("FATAL")
    def fatal(self, string):
        logging.fatal(string)

    # 是否隐藏NewModules的默认输出
    def display(self):
        loggingFormat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        loggingLeve = self.loggLevel[self.logging_level] if self.logging_dispaly.upper() == "DISPLAY" else logging.FATAL
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
        logging.basicConfig(format=loggingFormat, level=loggingLeve)

    # 恢复默认
    def hide(self):
        loggingFormat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        loggingLeve = self.loggLevel[self.logging_level]
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
        logging.basicConfig(format=loggingFormat, level=loggingLeve)

from pandas._libs.tslibs.timestamps import Timestamp as timestamp
import re
if __name__ == '__main__':
    # str = "^Ftp.*(\.py)$"
    reCompile = re.compile("goldCoins.*")
    print( not reCompile.search("goldCoins_invalid_571_*"))
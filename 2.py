# -* - coding: UTF-8 -* -
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('para2.conf')
#spark配置参数[spark_conf]
spark_host = config.get('spark_conf', 'spark_host')
spark_mode = config.get('spark_conf', 'spark_mode')
app_name = config.get('basic_stat', 'app_name')
read_data = config.get('basic_stat', 'read_data')
write_data = config.get('basic_stat', 'write_data')

sc = SparkContext(spark_mode, app_name)
sqlContext=SQLContext(sc)

taxi_df = sqlContext.load(spark_host+read_data)
taxi_df.show()

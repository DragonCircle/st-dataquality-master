# -* - coding: UTF-8 -* -
from pyspark import SparkContext, SparkConf
import ConfigParser
import time
#conf = SparkConf().setAppName(appName).setMaster(master)
#sc = SparkContext(conf=conf)

def to_datetime(time_interval):
	x = time.localtime(float(time_interval))
	return time.strftime('%Y-%m-%d %H:%M:%S',x)


config = ConfigParser.ConfigParser()
config.read('para2.conf')
#spark配置参数[spark_conf]
spark_host = config.get('spark_conf', 'spark_host')
spark_mode = config.get('spark_conf', 'spark_mode')
app_name = config.get('explore', 'app_name')

read_data = config.get('explore', 'read_data')
write_data = config.get('explore', 'write_data')

sc = SparkContext(spark_mode, app_name)
text_file = sc.textFile(spark_host+read_data)

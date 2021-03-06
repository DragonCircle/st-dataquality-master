# -* - coding: UTF-8 -* -
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
import ConfigParser
from datetime import datetime
#conf = SparkConf().setAppName(appName).setMaster(master)
#sc = SparkContext(conf=conf)

def to_datetime(time_interval):
	x = time.localtime(float(time_interval))
	return time.strftime('%Y-%m-%d %H:%M:%S',x)

def trans(p):
	p[0] = int(p[0])
	p[9] = datetime.strptime(p[9], "%Y-%m-%d %H:%M:%S")
	p[10] = float(p[10])
	p[11] = float(p[11])
	p[12] = float(p[12])
	p[13] = float(p[13])
	return p

config = ConfigParser.ConfigParser()
config.read('para2.conf')
#spark配置参数[spark_conf]
spark_host = config.get('spark_conf', 'spark_host')
spark_mode = config.get('spark_conf', 'spark_mode')
app_name = config.get('explore', 'app_name')

read_data = config.get('explore', 'read_data')
write_data = config.get('explore', 'write_data')
extract_fields_names = config.get('explore', 'extract_fields_names').split(",") #convert to list
extract_fields_indexes = map(int,config.get('explore', 'extract_fields_indexes').split(",")) # convert to int list

sc = SparkContext(spark_mode, app_name)
sqlContext=SQLContext(sc)
data = sc.textFile(spark_host+read_data)
parts = data.map(lambda l: l.split("|"))
parts_filter = parts.filter(lambda p:p[9]!="2010-01-01")
parts_trans = parts_filter.map(lambda p:trans(p))
taxi_row = parts_trans.map(lambda p:tuple(p[i] for i in  extract_fields_indexes))
taxi = taxi_row.map(lambda p: Row(**dict(zip(extract_fields_names,p)))) #pass dict to Rows()

schemaTaxi = sqlContext.createDataFrame(taxi)
schemaTaxi.registerTempTable("Taxi")
schemaTaxi.save(spark_host+write_data,mode="overwrite")

#print(schemaTaxi.head())
#taxi_log = Row(extract_fields_names)
#taxi = parts.map(lambda p: Row(taxi_id=p[0],gps_time=p[9],longitude=p[10],latitude=p[11],speed=p[12],direction=p[13]))
#schemaTaxi = sqlContext.inferSchema(t	axi)
#schemaTaxi.registerTempTable("taxi")
#driver1 = sqlContext.sql("SELECT gps_time FROM Taxi WHERE taxi_id = 23809")
#driver1.show()

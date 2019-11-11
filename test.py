from pyspark import *
from pyspark.sql import *
from pyspark.sql import *
from pyspark.sql.functions import explode, col
from pyspark.sql.types import *
from os import environ
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1,mysql:mysql-connector-java:8.0.16 pyspark-shell'


sc = SparkContext("local", "First App")
sqlContext = SQLContext(sc)

def XMLParsinfTBL_1():
    df = sqlContext.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='TEST_RESULTS') \
        .load('D:\Sample_files\parse.xml')
    df1 = df.drop('DATA').show()
    #df1.toPandas().to_csv("D:\Spark/spark_outputs/xmlParse_1.csv")
    #df.selectExpr('DATA.*').show()

def XMLParsingTBL_2():
    readxml = sc.textFile("D:\Sample_files\parse.xml")
    df = sqlContext.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag='TEST_RESULTS') \
        .load('D:\Sample_files\parse.xml')
    df_tbl = df.withColumn("DRIVE_CURRENTStruct",explode(col("DRIVE_CURRENT")))
    df_tbl.show()
    #df.select(col("c26Struct._VALUE").alias("value"),col("c26Struct._m").alias("option") ).show()




XMLParsingTBL_2()





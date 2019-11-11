from numpy.f2py.crackfortran import n
from pyspark import *
from pyspark.sql import *
from itertools import chain
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import col, explode, split, regexp_replace, arrays_zip, array, struct, explode_outer


def _sort_transpose_tuple(tup):
    x, y = tup
    return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]


def transpose(X):
    # validate
    if not isinstance(X, DataFrame):
        raise TypeError('X should be a DataFrame, not a %s'
                        % type(X))

    cols = X.columns
    n_features = len(cols)

    return X.rdd.flatMap(  # make into an RDD
        lambda xs: chain(xs)).zipWithIndex().groupBy(  # zip index
        lambda val_idx: val_idx[1] % n_features).sortBy(  # group by index % n_features as key
        lambda grp_res: grp_res[0]).map(  # sort by index % n_features key
        lambda grp_res: _sort_transpose_tuple(grp_res)).map(  # maintain order
        lambda key_col: key_col[1]).toDF()  # return to DF


sc = SparkContext("local", "First App")
readTRC = sc.textFile("D:\Spark\Samples\*.trc")


def TRCLoad_tbl1():
    rdd1 = readTRC.filter(lambda x: "=" in x)
    rdd2 = rdd1.map(lambda x: x.split('='))
    rdd3 = rdd2.groupByKey().map(lambda x: (x[0], list(x[1])))

    spark = SparkSession(sc)
    hasattr(rdd3, "toDF")
    rdd4 = rdd3.map(lambda x: Row(x[1])).toDF()
    final_rdd = transpose(rdd4).toDF("Product_Line", "IOP", "Date", "Serial_Number", "Station_Number",
                                     "Operator_Id", "Software_Version", "PIB_@_IOP", "CWL_@_IOP", "RMS_BW_@_IOP")
    # final_rdd.select(explode("Product_Line")).show()
    header = ("Product_Line", "IOP", "Date", "Serial_Number", "Station_Number",
                                     "Operator_Id", "Software_Version", "PIB_@_IOP", "CWL_@_IOP", "RMS_BW_@_IOP")

    combine = F.udf(lambda a, b, c, d, e, f, g, h, i, j: list(zip(a, b, c, d, e, f, g, h, i, j)),
                    ArrayType(StructType([StructField("Product_Line", StringType()),
                                          StructField("IOP", StringType()),
                                          StructField("Date", StringType()),
                                          StructField("Serial_Number", StringType()),
                                          StructField("Station_Number", StringType()),
                                          StructField("Operator_Id", StringType()),
                                          StructField("Software_Version", StringType()),
                                          StructField("PIB_@_IOP", StringType()),
                                          StructField("CWL_@_IOP", StringType()),
                                          StructField("RMS_BW_@_IOP", StringType())])))

    df1 = final_rdd.withColumn("new", combine("Product_Line", "IOP", "Date","Serial_Number", "Station_Number", "Operator_Id",
                                              "Software_Version", "PIB_@_IOP", "CWL_@_IOP", "RMS_BW_@_IOP")) \
        .withColumn("new", F.explode("new")) \
        .select(F.col("new.Product_Line").alias("Product_Line"), F.col("new.IOP").alias("IOP"),
                F.col("new.Date").alias("Date"), F.col("new.Serial_Number").alias("Serial_Number"), F.col(
            "new.Station_Number").alias("Station_Number"), F.col("new.Operator_Id").alias("Operator_Id"), F.col(
            "new.Software_Version").alias("Software_Version"), F.col("new.PIB_@_IOP").alias("PIB_@_IOP"), F.col(
            "new.CWL_@_IOP").alias("CWL_@_IOP"), F.col("new.RMS_BW_@_IOP").alias("RMS_BW_@_IOP"))
    df1.show()



TRCLoad_tbl1()


def TRCLoad_tbl2():
    rdd1 = readTRC.filter(lambda x: "=" not in x)
    rdd2 = rdd1.filter(lambda x: x is not '')
    rdd3 = rdd2.map(lambda x: x.replace("\t", ","))
    rdd4 = rdd3.map(lambda x: x.split(','))
    rdd5 = rdd4.filter(lambda x: 'Current(A)' not in x)
    hasattr(rdd5, "toDF")
    rdd5.toDF(["Current(A)", "PIB(%)", "CWL(nm)", "RMS BW(nm)"]).show()

TRCLoad_tbl2()

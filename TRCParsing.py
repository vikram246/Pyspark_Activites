from itertools import chain
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *


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
readTRC = sc.textFile("D:\Spark\Sample\AAA123-20170911-193101_par.trc")


def TRCParsing_tbl1():
    pyfilter = readTRC.filter(lambda x: "=" in x)
    pyfilter1 = pyfilter.map(lambda x: x.replace("=", ","))
    pyfilter2 = pyfilter1.map(lambda x: x.split(','))
    pyfilter3 = pyfilter2.filter(lambda x: "Product Line" not in x)
    spark = SparkSession(sc)
    hasattr(pyfilter3, "toDF")
    finalrddTodf = pyfilter3.map(lambda x: Row(x[1])).toDF()
    transpose(finalrddTodf).toDF("Date", "Product Line", "Serial Number", "Station Number",
                                 "Operator Id", "Software Version", "IOP", "PIB @ IOP", "CWL @ IOP",
                                 "RMS BW @ IOP").show()
    """ 
    The below line is to select the specific row for df
    # finalrddTodf3 = finalrddTodf2.filter(col("_1").rlike("2017/9/11 19:29:47"))
    # finalrddTodf3.show()
    """


def TRCParsing_tbl2():
    rdd2 = readTRC.filter(lambda x: "=" not in x)
    rdd3 = rdd2.map(lambda x: x.replace("=", ","))
    rdd4 = rdd3.filter(lambda x: x is not '')
    rdd5 = rdd4.map(lambda x: x.replace("\t", ","))
    rdd6 = rdd5.map(lambda x: x.split(','))
    rdd7 = rdd6.filter(lambda x: 'Current(A)' not in x)
    hasattr(rdd7, "toDF")
    rdd7.toDF(["Current(A)", "PIB(%)", "CWL(nm)", "RMS BW(nm)"]).show()


TRCParsing_tbl1()

TRCParsing_tbl2()

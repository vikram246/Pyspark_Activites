from itertools import chain

from numpy.ma import dump
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

    # Sorry for this unreadability...
    return X.rdd.flatMap( # make into an RDD
        lambda xs: chain(xs)).zipWithIndex().groupBy( # zip index
        lambda val_idx: val_idx[1] % n_features).sortBy( # group by index % n_features as key
        lambda grp_res: grp_res[0]).map( # sort by index % n_features key
        lambda grp_res: _sort_transpose_tuple(grp_res)).map( # maintain order
        lambda key_col: key_col[1]).toDF() # return to DF


sc = SparkContext("local", "First App")
rdd1 = sc.textFile("D:\Spark/test1.csv")

def CSVParsingTBL_1():
    rdd2 = rdd1.filter(lambda x: ",,,," in x)
    rdd3 = rdd2.map(lambda x: x.split(','))
    spark = SparkSession(sc)
    hasattr(rdd3, "toDF")
    rdd4 = rdd3.map(lambda x: Row(x[1])).toDF()

    rdd6 = transpose(rdd4).toDF("Device ID_1", "Test Start time_1", "Test Finish time_1", "Mask_1", "Lot_1", "Wafer",
                  "Strip", "Bar", "Row", "StationID", "Operator", "TestMode", "Customer", "Product",
                  "SpecName", "SpecRev", "SubProcess", "Test SW Version", "DateCruncherVersion", "DummyPwr", "NUll").show()
    rdd6.toPandas().to_csv("D:\Spark/spark_outputs/file3.csv")

    """ 
       The below line is to select the specific row for df
    # rdd7 =  rdd6.filter(col("_1").like("NA"))
    # rdd7.show()
    """



def CSVParsingTBL_2():
    headerList_tbl2 = ['MZ#', 'PATH', 'CHAN', 'ParamName', 'ParamVal', 'TestType']
    filter_rdd1 = rdd1.filter(lambda x: ",,,," not in x)
    filter_rdd2 = filter_rdd1.map(lambda a: a.split(','))
    filter_rdd3 = filter_rdd2.filter(lambda x: 'MZ#' not in x)
    spark = SparkSession(sc)
    hasattr(filter_rdd3, "toDF")
    final_rdd = filter_rdd3.toDF(headerList_tbl2).show()
    # final_rdd.toPandas().to_csv("D:\Spark/file.csv")

CSVParsingTBL_1()


CSVParsingTBL_2()
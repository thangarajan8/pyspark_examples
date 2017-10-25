LINE_LENGTH = 200
def print_horizontal():
    """
    Simple method to print horizontal line
    :return: None
    """
    for i in range(LINE_LENGTH):
        sys.stdout.write('-')
    print("")


import sys
from pyspark import SQLContext
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Row_Number')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sparkContext=sc)
parquetFile = sqlContext.read.parquet("../data/nation.plain.parquet")
parquetFile.registerTempTable("parquetFile")
nations_all_sql = sqlContext.sql("SELECT * FROM parquetFile")
nations_all_sql.show()


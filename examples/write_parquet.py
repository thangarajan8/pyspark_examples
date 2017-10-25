from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf1 = SparkConf().setAppName('Row_Number')
sc1 = SparkContext(conf=conf1)
sql_context = SQLContext(sc1)
csv_file_path = '../data/emp.csv'
employee_rdd = sc1.textFile(csv_file_path).map(lambda line : line.split(','))
employee_df = employee_rdd.toDF(['dept','ctc'])
employee_df.show()

employee_df.write.parquet("../data/emp.parquet")
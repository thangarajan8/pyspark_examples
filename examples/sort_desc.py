from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf1 = SparkConf().setAppName('sort_desc')
sc1 = SparkContext(conf=conf1)
sql_context = SQLContext(sc1)
csv_file_path = '../data/emp.csv'
employee_rdd = sc1.textFile(csv_file_path).map(lambda line: line.split(','))
print(type(employee_rdd))
employee_rdd_sorted = employee_rdd.sortByKey(ascending= False)
employee_df = employee_rdd.toDF(['dept','ctc'])
employee_df_sorted = employee_rdd_sorted.toDF(['dept','ctc'])

employee_df.show()
employee_df_sorted.show()
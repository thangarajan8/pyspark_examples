from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf1 = SparkConf().setAppName('Aggr')
sc1 = SparkContext(conf=conf1)
sql_context = SQLContext(sc1)
csv_file_path = 'C:\\Thanga\\Work\\python\\uni\\pyspark_examples\\emp.csv'
employee_rdd = sc1.textFile(csv_file_path).map(lambda line : line.split(','))
employee_df = employee_rdd.toDF(['dept','ctc'])
employee_df.show()
employee_df.createOrReplaceTempView("salary")
sql_context.sql("show tables").show()
salary_grouped = sql_context.sql("select dept, sum(ctc) from salary group by dept")
salary_grouped.show()


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf1 = SparkConf().setAppName('Ranking')
sc1 = SparkContext(conf=conf1)
sql_context = SQLContext(sc1)
csv_file_path = '../data/ranking.csv'
sql_query_1 = "select TO_DATE(CAST(UNIX_TIMESTAMP(orig_date, 'MM/dd/yyyy') AS TIMESTAMP)) as original_date, field1, " \
              "field2 from rank_hcc"
rank_rdd = sc1.textFile(csv_file_path).map(lambda line : line.split(','))
ranking_df = rank_rdd.toDF(['orig_date','field1','field2','rank'])
# ranking_df.show()

ranking_df.createOrReplaceTempView("rank_hcc")
# sql_context.sql("show tables").show()
converted_rank = sql_context.sql(sqlQuery= sql_query_1)
converted_rank.createOrReplaceTempView('rank_hcc_1')

sql_context.sql("SELECT original_date,field1,field2, RANK() OVER (ORDER BY field1+field2,original_date ) AS rank FROM rank_hcc_1").show()


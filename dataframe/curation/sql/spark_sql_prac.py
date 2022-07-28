from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    fin_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    finance_df = spark.read.parquet(fin_file_path)
    finance_df.createOrReplaceTempView("raw_finances")
    finance_df.printSchema()


   # finance_df.orderBy("Date").show(6)
   # finance_df.groupBy("AccountNumber").orderBy("Date").show(6)

finance_df= spark.sql("select * from raw_finances order by amount LIMIT 5").show()

#finance_df=spark.sql("select count(*) from raw_finances").show()

    spark.sql("""
          select
            AccountNumber,
            Amount,
            to_date(cast(unix_timestamp(Date, 'MM/dd/yyyy') as timestamp)) as Date,
            Description
          from
            raw_finances
          """)\
        .createOrReplaceTempView("finances1")

    spark.sql("""
              select
                AccountNumber,
                Amount,
                Date,
                Description,
                avg(Amount) over (partition by AccountNumber order by Date rows between 4 preceding and 0 following) as RollingAvg
              from
                finances1
              """) \
        .show(5, False)



# finance_df= spark.sql("select * from raw_finances order by amount")
# finance_df.repartition(1)\
#     .write\
#     .mode("overwrite")\
#     .option("header","true")\
#     .option("delimiter","|")\
#     .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/fin_out3")

# spark.sql("""
#           select
#             AccountNumber,
#             Amount,
#             Date,
#             Description,
#             avg(Amount) over (partition by AccountNumber order by Date rows between 4 preceding and 0 following) as RollingAvg
#           from
#             finances
#           """)\
#         .show(5, False)


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/spark_sql_prac.py


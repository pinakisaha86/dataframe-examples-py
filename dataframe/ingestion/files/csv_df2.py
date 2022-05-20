from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
        # .master('local[*]') \
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

    print("\nCreating dataframe ingestion CSV file using 'SparkSession.read.format()'")

fea_schema = StructType() \
        .add("Store", IntegerType(), True) \
        .add("Date", string(), True) \
        .add("Temperature", string(), True) \
        .add("Fuel_Price", string(), True) \
        .add("MarkDown1", string(), True) \
		.add("MarkDown2", string(), True) \
        .add("MarkDown3", string(), True) \
        .add("MarkDown4", string(), True) \
        .add("MarkDown5", string(), True) \
		.add("CPI", string(), True) \
        .add("Unemployment", string(), True) \
        .add("IsHoliday", string(), True)

features_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fea_schema) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/features.csv")

#   features_df = spark.read \
#        .option("mode", "DROPMALFORMED") \
#        .option("header", "false") \
#        .option("delimiter", ",") \
#        .option("inferSchema", "true") \
#        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/features.csv")

features_df.printSchema()

features_df.limit(3).show()

#features_df.write.partitionBy("Fuel_Price").csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/features_df")
#features_df\
#        .repartition(2) \
#        .write \
#        .partitionBy("_c0") \
#        .mode("overwrite") \
#        .option("header", "true") \
#        .option("delimiter", ",") \
#        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/features")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/csv_df2.py
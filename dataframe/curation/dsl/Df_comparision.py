# Requisite packages to import
import sys
from pyspark.sql.functions import lit, count, col, when
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')


# Create the two dataframes
df1 = spark.createDataFrame([(11,'Sam',1000,'ind','IT','2/11/2019'),(22,'Tom',2000,'usa','HR','2/11/2019'),
                                 (33,'Kom',3500,'uk','IT','2/11/2019'),(44,'Nom',4000,'can','HR','2/11/2019'),
                                 (55,'Vom',5000,'mex','IT','2/11/2019'),(66,'XYZ',5000,'mex','IT','2/11/2019')],
                                 ['No','Name','Sal','Address','Dept','Join_Date'])
df2 = spark.createDataFrame([(11,'Sam',1000,'ind','IT','2/11/2019'),(22,'Tom',2000,'usa','HR','2/11/2019'),
                                  (33,'Kom',3000,'uk','IT','2/11/2019'),(44,'Nom',4000,'can','HR','2/11/2019'),
                                  (55,'Xom',5000,'mex','IT','2/11/2019'),(77,'XYZ',5000,'mex','IT','2/11/2019')],
                                  ['No','Name','Sal','Address','Dept','Join_Date'])
df1 = df1.withColumn('FLAG',lit('DF1'))
df2 = df2.withColumn('FLAG',lit('DF2'))

# Concatenate the two DataFrames, to create one big dataframe.
df = df1.union(df2)
df.show()
#window func to identify the duplicates

my_window = Window.partitionBy('No','Name','Sal','Address','Dept','Join_Date').rowsBetween(-sys.maxsize, sys.maxsize)
df = df.withColumn('FLAG', when((count('*').over(my_window) > 1),'SAME').otherwise(col('FLAG'))).dropDuplicates()
df.show()


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/Df_comparision.py

#output
# +---+----+----+-------+----+---------+----+
# | No|Name| Sal|Address|Dept|Join_Date|FLAG|
# +---+----+----+-------+----+---------+----+
# | 11| Sam|1000|    ind|  IT|2/11/2019| DF1|
# | 22| Tom|2000|    usa|  HR|2/11/2019| DF1|
# | 33| Kom|3500|     uk|  IT|2/11/2019| DF1|
# | 44| Nom|4000|    can|  HR|2/11/2019| DF1|
# | 55| Vom|5000|    mex|  IT|2/11/2019| DF1|
# | 66| XYZ|5000|    mex|  IT|2/11/2019| DF1|
# | 11| Sam|1000|    ind|  IT|2/11/2019| DF2|
# | 22| Tom|2000|    usa|  HR|2/11/2019| DF2|
# | 33| Kom|3000|     uk|  IT|2/11/2019| DF2|
# | 44| Nom|4000|    can|  HR|2/11/2019| DF2|
# | 55| Xom|5000|    mex|  IT|2/11/2019| DF2|
# | 77| XYZ|5000|    mex|  IT|2/11/2019| DF2|
# +---+----+----+-------+----+---------+----+
#
# +---+----+----+-------+----+---------+----+
# | No|Name| Sal|Address|Dept|Join_Date|FLAG|
# +---+----+----+-------+----+---------+----+
# | 33| Kom|3500|     uk|  IT|2/11/2019| DF1|
# | 77| XYZ|5000|    mex|  IT|2/11/2019| DF2|
# | 55| Xom|5000|    mex|  IT|2/11/2019| DF2|
# | 44| Nom|4000|    can|  HR|2/11/2019|SAME|
# | 11| Sam|1000|    ind|  IT|2/11/2019|SAME|
# | 33| Kom|3000|     uk|  IT|2/11/2019| DF2|
# | 22| Tom|2000|    usa|  HR|2/11/2019|SAME|
# | 66| XYZ|5000|    mex|  IT|2/11/2019| DF1|
# | 55| Vom|5000|    mex|  IT|2/11/2019| DF1|
# +---+----+----+-------+----+---------+----+
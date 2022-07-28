from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from model.Role import Role
from model.Employee import Employee

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

data1 = [{'Name':'Jhon','ID':21.528,'Add':'USA'},{'Name':'Joe','ID':3.69,'Add':'USA'},
         {'Name':'Tina','ID':2.48,'Add':'IND'},{'Name':'Jhon','ID':22.22, 'Add':'USA'},
         {'Name':'Joe','ID':5.33,'Add':'INA'}]

products = spark.createDataFrame(data1)
products.show()

data2 = [{'Name':'Jhon','ID':21.528,'Add':'USA'},{'Name':'Joe','ID':3.69,'Add':'USeA'},{'Name':'Tina','ID':2.48,'Add':'IND'},
         {'Name':'Jhon','ID':22.22, 'Add':'USdA'},{'Name':'Joe','ID':5.33,'Add':'rsa'}]

prod = spark.createDataFrame(data2)

prod.show()

e = broadcast(products)

f = prod.join(broadcast(e),prod.Add == e.Add)
f.show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/broadcast_prac.py
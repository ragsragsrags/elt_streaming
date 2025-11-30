import pyodbc

# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col

import json
# import os
import sys

print("__name__: ", __name__)

if __name__ == "__main__":
    print("sys.argv: ", sys.argv)
    config_file = ""

    if len(sys.argv) < 2:
        print("Usage: spark-submit replicate-testdb-data.py <config file> <preload>")
        sys.exit(1)
    else:
        config_file = sys.argv[1]

    print("config_file: ", config_file)

    f = open(config_file,)
    config = json.load(f)
    f.close()

    source = config["source"]
    destination = config["destination"]
    processNoOfRows = config["processNoOfRows"]
    sparkMaster = config["sparkMaster"]
    sparkJars = config["sparkJars"]
    tables = config["tables"]
    sparkExecutorMemory = config["sparkExecutorMemory"]
    sparkDriverMemory = config["sparkDriverMemory"]
    sparkExecutorCores = config["sparkExecutorCores"]
    sparkCoresMax = config["sparkCoresMax"]
    offsetPreloadTimestamp = config["offsetPreloadTimestamp"]
    pyodbcUrl = destination["pyodbc_url"]

    print("source: ", source)
    print("destination: ", destination)
    print("processNoOfRows: ", processNoOfRows)
    print("sparkMaster: ", sparkMaster)
    print("sparkJars: ", sparkJars)
    print("tables: ", tables)
    print("sparkExecutorMemory: ", sparkExecutorMemory)
    print("sparkDriverMemory: ", sparkDriverMemory)
    print("sparkExecutorCores: ", sparkExecutorCores)
    print("sparkCoresMax: ", sparkCoresMax)
    print("offsetPreloadTimestamp: ", offsetPreloadTimestamp)
    print("pyodbcUrl: ", pyodbcUrl)
    
    conn = pyodbc.connect(pyodbcUrl)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"EXEC [dbo].[ProcessMessages] {processNoOfRows}")
    cursor.close()
    conn.close()

# code for spark
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col

# import json
# import os
# import sys

# print("__name__: ", __name__)

# if __name__ == "__main__":
#     print("sys.argv: ", sys.argv)
#     config_file = ""

#     if len(sys.argv) < 2:
#         print("Usage: spark-submit replicate-testdb-data.py <config file> <preload>")
#         sys.exit(1)
#     else:
#         config_file = sys.argv[1]

#     print("config_file: ", config_file)

#     f = open(config_file,)
#     config = json.load(f)
#     f.close()

#     source = config["source"]
#     destination = config["destination"]
#     processNoOfRows = config["processNoOfRows"]
#     sparkMaster = config["sparkMaster"]
#     sparkJars = config["sparkJars"]
#     tables = config["tables"]
#     sparkExecutorMemory = config["sparkExecutorMemory"]
#     sparkDriverMemory = config["sparkDriverMemory"]
#     sparkExecutorCores = config["sparkExecutorCores"]
#     sparkCoresMax = config["sparkCoresMax"]
#     offsetPreloadTimestamp = config["offsetPreloadTimestamp"]

#     print("source: ", source)
#     print("destination: ", destination)
#     print("processNoOfRows: ", processNoOfRows)
#     print("sparkMaster: ", sparkMaster)
#     print("sparkJars: ", sparkJars)
#     print("tables: ", tables)
#     print("sparkExecutorMemory: ", sparkExecutorMemory)
#     print("sparkDriverMemory: ", sparkDriverMemory)
#     print("sparkExecutorCores: ", sparkExecutorCores)
#     print("sparkCoresMax: ", sparkCoresMax)
#     print("offsetPreloadTimestamp: ", offsetPreloadTimestamp)
    
#     # Initialize Spark Session
#     def get_spark_session():
#         spark = (   
#                 SparkSession
#                     .builder
#                     .config(
#                             "spark.driver.host", 
#                             "localhost"
#                         )
#                     .master(sparkMaster)
#                     .config("spark.jars", sparkJars)
#                     .config("spark.executor.memory", sparkExecutorMemory)
#                     .config("spark.driver.memory", sparkDriverMemory)
#                     .config("spark.executor.cores", sparkExecutorCores)
#                     .config("spark.cores.max", sparkCoresMax)
#                     .appName("InsertTestDBMessages")
#                     .getOrCreate()
#         )

#         spark.sparkContext.setLogLevel('WARN')

#         return spark
    
#     def stop_spark_session():
#         active_spark_session = SparkSession.getActiveSession()
        
#         if active_spark_session:
#             active_spark_session.stop()

#     def set_spark_url(spark, url):
#         # establish connection
#         df = (
#             spark.read
#                 .format("jdbc")
#                 .option("url", url)
#                 .option("dbtable", "(SELECT Temp = 1) AS Temp")
#                 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
#                 .load()
#         )    

#     # for table in tables:
#     spark = get_spark_session()
#     set_spark_url(spark, destination["url"])
#     connection = spark._jvm.java.sql.DriverManager.getConnection(destination["url"])

#     # process messages
#     processMessagesSql = f"EXEC [dbo].[ProcessMessages] ?"
#     processStatement = connection.prepareStatement(processMessagesSql)
#     processStatement.setInt(1, processNoOfRows)
#     processStatement.executeUpdate()


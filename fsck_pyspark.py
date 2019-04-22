# Author: Shashank Naik

# ********************************DESCRIPTION************************************

# This script is part of the process to load the fsck data into a hive/impala table
# The script runs automatically once the shell script - fsck_prepare.sh is run

# *******************************************************************************

from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import datetime

spark=SparkSession.builder.getOrCreate()
curr_date = str(datetime.date.today())
hdfs_path_to_csv='/hdfs/path/to/processed/fsck/extract' + 'fsck_allBlockFiles_' + curr_date + ".csv"

# The database and table name
dbName = "<database_name>"
tblName = "<table_name>"

csvDF = spark.read.option("header","false").csv(hdfs_path_to_csv).toDF("Path","FileSize","BlocksCount")
csvDF = csvDF.select("Path", "BlocksCount", "FileSize").filter("BlocksCount != 0")

def splitPaths(str):
  index= 1
  paths = []
  while (index > 0):
    paths.append(str[:index])
    index = str.find("/", index+1)
  return paths

splitPathsUDF = udf(splitPaths, ArrayType(StringType(),False))
explodedPaths = csvDF.withColumn("Path", explode(splitPathsUDF(csvDF["Path"])))
explodedPaths.createOrReplaceTempView("explodedpaths")

smallBlocksListDF = spark.sql("SELECT Path, sum(FileSize)/sum(BlocksCount)/1048576 as avgblocksize, sum(FileSize)/1048576 as TotalSize, sum(BlocksCount) as totalblocks, " +
  " cast(current_date as string) as extract_dt " + " from explodedpaths GROUP BY path")

filteredPaths = smallBlocksListDF.filter("path not like '/user/oozie%'").filter("path not like '/solr%'").filter("path not like '/hbase%'").filter("path not like '/tmp%'").filter("path not like '/user/hive/warehouse%'")

filteredPaths.repartition(1).write.mode('append').format('parquet').saveAsTable(dbName+"."+tblName, partitionBy='extract_dt', compression= 'snappy')

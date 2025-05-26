# Spark & Other Examples with Implementation techniques (Refer src folder)
## Concepts implemented:

- Partition By vs Bucket By (1_partitionby_bucketby)
- Spark aggregation (2_spark_agg_examples)

# Unity Catalog OSS

### Launch spark-sql with Unity Catalog (Open Source) Support

- Unity Catalog Open Source example (3_uc_oss_loadtable)
- The below config points to a Catalog called 'demo' which was created in Unity Catalog UI after installing it locally
- For this to work, Unity Catalog should be installed and started prior to Spark session (in this case while trying to use Spark SQL). [Refer here ](https://github.com/unitycatalog/unitycatalog)
- To run spark-sql using the open-source Unity Catalog, Delta Lake (S3 integration optional):

<pre><code>spark-sql  --name "local-uc-test"  --master "local[*]"  --packages "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0"  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog"  --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"  --conf "spark.sql.catalog.demo=io.unitycatalog.spark.UCSingleCatalog" --conf "spark.sql.catalog.demo.uri=http://localhost:8080" --conf "spark.sql.catalog.demo.token=" --conf "spark.sql.defaultCatalog=demo" </code></pre>


### Run spark code using spark-submit to load a UC table:
<pre><code>spark-submit     --packages io.unitycatalog:unitycatalog-spark_2.12:0.2.0,io.delta:delta-spark_2.12:3.2.1     --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"     --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog"     --conf "spark.sql.catalog.demo=io.unitycatalog.spark.UCSingleCatalog"     --conf "spark.sql.catalog.demo.uri=http://localhost:8080"     --conf "spark.sql.catalog.demo.token="     --conf "spark.sql.defaultCatalog=demo"     src/3_uc_oss_loadtable.py </code></pre>

### Creating UC tables with explicit DDL
- Even if external tables can be created using delta in pyspark, UC OSS still requires manual DDL and column definition so that metadata is properly captured in the UI. Otherwise, when using pyspark alone, the metadata is available only through DESCRIBE command in CLI

<pre><code>~/spark_env/unitycatalog$ bin/uc table create \
    --full_name demo.employee.address_v2 \
    --storage_location /home/obiwan/dbverse_git/pyspark_examples/src/processed/uc_address \
    --format delta \
    --columns "AddressID INT, AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvince STRING, CountryRegion STRING, PostalCode STRING, rowguid STRING, ModifiedDate TIMESTAMP" </code></pre>




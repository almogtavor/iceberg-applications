# Kafka2Iceberg

A microservice that reads from Kafka and writes to Iceberg. 
Meant to run in a distributed manner so the writes will work in batch.
Kafka2Iceberg is inspired by Kafka Connect, but isn't a plugin to Kafka Connect, since it is preferable to write to Iceberg with Spark.

## Design and Implementation Details

Iceberg works with a concept of Catalog.
In Iceberg catalogs are used to load, create, and manage Iceberg tables. 
A catalog can be based on Hive / Hadoop HDFS API (behind the scenes it can be S3).
The Hive catalog connects to a Hive metastore to keep track of Iceberg tables.
A Hadoop catalog doesn’t need Hive MetaStore, but HDFS or similar file systems that support atomic rename. 
**Concurrent writes with a Hadoop catalog are not safe with a local FS or S3.**
We've currently decided to support the Hadoop Catalog.

Relevant links:
* https://stackoverflow.com/questions/73361391/write-apache-iceberg-table-to-azure-adls-s3-without-using-external-catalog?rq=1
* https://iceberg.apache.org/spark-quickstart/

Nessie is also an interesting possibility (Experimentation):
Changes to a table can be tested in a branch before merging back into main. 
This is particularly useful when performing large changes like schema evolution or partition evolution. 
A partition evolution could be performed in a branch, and you would be able to test out the change (eg performance benchmarks) before merging it. 
This provides great flexibility in performing on-line table modifications and testing without interrupting downstream use cases. 
If the changes are incorrect or not performant the branch can be dropped without being merged.

### Using Trino

Trino works with a directory named `etc`, in which we will configure everything, as well as a directory `conf` for the hive metastore.
For the local development, note that the default user for the web UI is `trino`. 
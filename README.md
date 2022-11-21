# Iceberg Applications

A collection of out-of-the-box Spring Boot based Apache Spark applications that perform common tasks regarding Apache Iceberg. 
Currently the existing applications are:
* `kafka2iceberg` - A pipeline that reads data from Kafka and writes to Iceberg.
* `iceberg-maintainer` - A program that executes Iceberg maintaining tasks.

## Local Usage & Development,

The local usage & development of `iceberg-application` requires to set up containers using docker compose.
For the general environment it is required to set up `environment/compose/environment-docker-compose.yaml`.
This will bring up Minio S3, Kafka & Zookeeper (with Kafka UI).
Based on how we would like to configure Iceberg's catalog, we should also bring up `environment/compose/{nessie/postgres}-docker-compose.yaml`.
Or in case of using S3 based catalog (e.g. Hadoop catalog), we don't need any other container.

Each application needs to be configured by `spring.iceberg.catalog-type={hadoop/hive/jdbc}` to choose the catalog type.
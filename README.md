# Iceberg Applications 🧊

A collection of out-of-the-box Spring Boot based Apache Spark applications that perform common tasks regarding Apache Iceberg. 
Currently, the existing applications are:
* `kafka2iceberg` - A pipeline that reads data from Kafka and writes to Iceberg.
* `iceberg-maintainer` - A program that executes Iceberg maintenance tasks.

<details>
<summary>
For Local Usage & Development:
</summary>

## Local Usage & Development

### Step 1: Set Up the Environment Using Docker Compose

To run iceberg-application locally, you need to set up the required environment using Docker Compose.

#### General Environment:

Use the Docker Compose file located at environment/compose/environment-docker-compose.yaml.
This setup includes MinIO S3, Kafka, and Zookeeper (with Kafka UI).
#### Iceberg Catalog Setup:

Depending on your Iceberg catalog configuration, bring up one of the following Docker Compose files:
* `environment/compose/nessie-docker-compose.yaml` (for Nessie catalog)
* `environment/compose/postgres-docker-compose.yaml` (for Postgres-based catalog)
* If you are using an S3-based catalog (e.g., Hadoop catalog), no additional containers are required.

#### Configuration:

Configure each application in the Spring `application.yaml` file. Set the catalog type using `spring.iceberg.catalog-type={hadoop/hive/jdbc}`.

### Step 2: Produce Data to Kafka
Run the [DevSamplePojoKafkaProducer.java](kafka2iceberg%2Fsrc%2Fmain%2Fjava%2Fio%2Fgithub%2Falmogtavor%2FDevSamplePojoKafkaProducer.java) script to produce sample data to Kafka.

### Step 3: Execute the Kafka2Iceberg Service

#### 1. Hadoop Setup:

Download the [Hadoop Binaries](https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin) and place them locally at `C:/hadoop`.
Ensure the binaries are located at `C:/hadoop/hadoop-2.7.1`.
Environment Variables:

In your IntelliJ run configurations, set the following environment variables:
`HADOOP_HOME=C:\hadoop\hadoop-2.7.1;PATH=C:\hadoop\hadoop-2.7.1\bin`

#### 2. Spring Boot Profile:

Set the Spring Boot profile to either `jdbc` or `nessie`, depending on your catalog type.

#### 3. VM Options:

Set the VM options to: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --enable-preview`.

### Step 4: View your Iceberg Table at the Minio console

Enter `locahost:9001`, and checkout your bucket to verify the Kafka2Iceberg have successfully created an Iceberg table:
![img.png](docs/minio_example.png)


### Step 5: Run the Iceberg Maintainer
* Run the iceberg-maintainer application in the same manner as Kafka2Iceberg.
* After the files have been merged, check your MinIO bucket again to see the changes.

</details>
# Iceberg Maintainer

Iceberg compactor is an out-of-the-box implementation of an Apache Spark job that uses Apache Iceberg library for compaction of Iceberg tables.
The job can easily become an "Iceberg Maintainer", and do other important background tasks like snapshots expiring.

The maintenance tasks:
* Data files **compaction**
* Manifests **rewrite**
* **Snapshots expiring**
* **Orphan files** deletion



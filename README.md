# Oracle to Hadoop Data Lake ETL using Scala-Spark

![Project Image](https://github.com/wajidturi50/ETL_Oracle_to_Hadoop_DataLake/raw/main/Final%20Results.png)

## Requirements

- **Scala 3.3 LTS** (Long-Term Support)
- Apache Spark (with Scala 3.3 compatible distribution)
- Apache Kudu (with Scala 3 compatible client libraries)
- Oracle JDBC driver (ojdbc6)

## Explanation

Welcome to the Oracle to Hadoop Data Lake ETL project! This powerful ETL (Extract, Transform, Load) solution aims to efficiently extract data from multiple tables in an Oracle database and ingest it into a Hadoop data lake using PySpark.

The project utilizes PySpark, a Python library that provides an interface for Apache Spark, a fast and general-purpose cluster computing system. PySpark enables seamless integration with Hadoop's distributed file system and allows us to process and transform large datasets in parallel.

To enhance the ETL process, the project incorporates configuration with Kudu nodes and Impala. Kudu is a columnar storage manager for Hadoop that enables fast analytics on fast data. Impala, on the other hand, is a massively parallel processing (MPP) SQL query engine for Hadoop that provides real-time, interactive SQL queries on large datasets.

## Scala Version

This project targets **Scala 3.3 LTS** for long-term support and stability. The ETL script (`ETL.scala`) is executed via `spark-shell` using Scala 3.3 compatible JAR dependencies as configured in `ETL.sh`.

> **Note:** The project was previously built against Scala 2.11. All dependency references in `ETL.sh` have been updated to use Scala 3 cross-compiled artifacts (suffix `_3`).

## Contact

If you have any questions or suggestions, feel free to reach out to me at [wajidturi7@gmail.com].

Thank you for visiting this repository and happy coding!

# AdvancedDatabasesNTUA23
Final project of the course Advanced Databases - 9th semester, Electrical Computer Engineering, National Technical University of Athens.

Series of queries on big data regarding the basic dataset of Los Angeles Crime Data [2010-2010](https://catalog.data.gov/dataset/crime-data-from-2010-to-2019)and [2020-2023](https://catalog.data.gov/dataset/crime-data-from-2020-to-present). Project runs on Apache Spark over Apache Hadoop Yarn using HDFS. All queries are implemented using Python PySpark.

Detail description in the form of a report can be found [here](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/03119111_03119442.pdf).

Project assignment can be found [here](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/advanced_db_project.pdf).

# Contributors
Vidali Despoina - 03119111 ([despoinavdl](https://github.com/despoinavdl)) 

Arkadopoulou Eleftheria - 03119442 ([adamkapetis](https://github.com/adamkapetis)) 


# Installation and execution
```bash
git clone https://github.com/despoinavdl/AdvancedDatabasesNTUA23
```
Set up VMs and download data as described in the [assignment](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/advanced_db_project.pdf).

Run 
```bash
start-dfs.sh
start-yarn.sh
$SPARK_HOME/sbin/start-history-server.sh
```
```bash
mkdir /data
```
from the master node.

Upload data to HDFS
```bash
hdfs dfs -put /path/to/local/directory hdfs://okeanos-master:54310/data
```

Run queries using 
```bash
spark-submit <query.py>
```


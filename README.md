# AdvancedDatabasesNTUA23
Final project of the course Advanced Databases - 9th semester, Electrical Computer Engineering, National Technical University of Athens.

Series of queries regarding big data on the datasets 'Los Angeles Crime Data', [2010-2010](https://catalog.data.gov/dataset/crime-data-from-2010-to-2019) and [2020-2023](https://catalog.data.gov/dataset/crime-data-from-2020-to-present). 
Project runs on Apache Spark over Apache Hadoop Yarn using HDFS. All queries are implemented using Python PySpark.

Project assignment can be found [here](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/advanced_db_project.pdf).

Detailed description in the form of a report can be found [here](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/03119111_03119442.pdf).

Queries can be found in the [queries file](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/tree/main/queries).


# Contributors
Vidali Despoina - 03119111 ([despoinavdl](https://github.com/despoinavdl)) 

Arkadopoulou Eleftheria - 03119442 ([fleria](https://github.com/adamkapetis)) 


# Installation and execution
```bash
git clone https://github.com/despoinavdl/AdvancedDatabasesNTUA23
```
Set up VMs and download data as described in the [assignment](https://github.com/despoinavdl/AdvancedDatabasesNTUA23/blob/main/advanced_db_project.pdf).

Run 
```bash
./start_vm.sh
```
from the master node to start all services

Upload data to /data in HDFS
```bash
hdfs dfs -put /path/to/local/directory hdfs://okeanos-master:54310/data
```

Run queries using 
```bash
spark-submit <query_number.py>
```


# Basic Airflow 
Credits to Airflow Tutorial [Sample Youtube Tutorial](https://www.youtube.com/watch?v=EySWDTZBZtM&list=PLMmVziys3riDnnI2qATwy6ObDRvZ546K3) . Basic example of setting up and using airflow is learning through this guide. 

Basic Airflow functionalities tested are as follows :
1. Basic Direct DAG run with DAG and Code in the same script
2. Standard DAG run with DAG and Code in different script ( Through extra function to link all 3 objects, in order to pass the output dataset from extract to transform, then from transform to load )
4. XComs are not tested as the passing of data among stage has its limitation. Complex items, e.g. DataFrame are unable to be passed over, as it is unable to be serialize

# Issues Encountered when running airflow
## Setting up parallel run
As default airflow database is sqlite, multidag parallel run is impossible. The database for airflow is adjusted to MySQL instead.
The MySQL database is setup following the instructions from [Official Backend Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)，but several more adjustments are required.

- Issue occurs as MySQL server and workbench are both in Windows but airflow is in WSL2 Ubuntu, so further adjustment in airflow.cfg is needed. 
1. executor = LocalExecutor ( by default it is originally Sequential Executor )
2. host ip address ( the <host> part in sql_alchemy_conn = mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname> need to be the IPv4 address in Windows,
                     this can be checked with the `ipconfig` command in PowerShell. )

- In scenario whereby `airflow db init` failed due to 'dag_id' and referenced column 'dag_id' in foreign key constraint 'task_fail_ti_fkey' are incompatible, it was resolved by removing the airflow database in MySQL and `airflow db init` again. 

- Also added additional inbound rule at firewall 
>   - Configure Windows Firewall: <br>
    - Open Windows Defender Firewall <br>
    - Click on "Advanced settings" <br>
    - Create a new Inbound Rule <br>
    - Choose "Port" and specify TCP port 3306 (default MySQL port) <br>
    - Allow the connection <br>
    - Apply to Domain, Private, and Public networks <br>
    - Name the rule (e.g., "MySQL for WSL")

# Successful Result
Additional screenshot for running MultiDAG run recorded
![Successful Airflow MultiDAG Run Screenshot](https://github.com/user-attachments/assets/eaeb518e-486d-4a99-8e36-21816bc22dde)

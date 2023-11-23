Setup Spark on Windows 11
1.  Download spark latest version from https://spark.apache.org/downloads
    Spark version used for this repo : spark-3.4.1-bin-hadoop3
![img.png](img.png)
2.  Download winutils.exe and hadoop.ddl from https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
![img_2.png](img_2.png)
3. Unzip spark to local directory on windows 11
4. ![img_1.png](img_1.png)
5. Copy winutils.exe to local directory
6. ![img_3.png](img_3.png)
7. copy hadoop.ddl to C:\Windows\System32
8. ![img_4.png](img_4.png)
9. Add environment variables for SPARK_HOME and HADOOP_HOME
10. ![img_5.png](img_5.png)
11. Edit Path variable
12. ![img_6.png](img_6.png)
13. Add %SPARK_HOME%\bin and %HADOOP_HOME%\bin as shown below to Path variable
14. ![img_7.png](img_7.png)
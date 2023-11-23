package org.cloud.data.incremental;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


public class SparkHudiIncrementalReadData_5 {
    public static void main(String[] args) {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        System.setProperty("java.library.path","D:\\sparksetup\\hadoop\\bin");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("className", "org.apache.hudi")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.sql.warehouse.dir", "file:///C:/tmp/spark_shell/spark_warehouse");
        SparkSession spark = SparkSession.builder().appName("Spark Hudi Read").config(sparkConf).getOrCreate();

        //Read table data
        Dataset trips_df = spark.read().format("org.apache.hudi").load("file:///D:/sparksetup/sparkdata/employee_table");
        trips_df.createOrReplaceTempView("employee_table");
        spark.sql("select distinct(_hoodie_commit_time) as commitTime from employee_table order by commitTime").show();

        //Get the last commit timestamp
        String[] _hoodie_commit_time = spark.sql("select distinct(_hoodie_commit_time) as commitTime from employee_table order by commitTime limit 1")
                .collectAsList().stream().map(row->row.getString(0)).toArray(String[]::new);
        System.out.print(" Latest commit time is : "+_hoodie_commit_time);

        // Show records modified from begin time _hoodie_commit_time i.e modified and added record only --> time travel
        spark.read().format("org.apache.hudi").option("hoodie.datasource.query.type", "incremental")
                . option("hoodie.datasource.read.begin.instanttime", _hoodie_commit_time[0])
                .load("file:///D:/sparksetup/sparkdata/employee_table").show();

        //Show records from begin commit : set `hoodie.datasource.read.begin.instanttime=0`  and omit `hoodie.datasource.read.end.instanttime`
        spark.read().format("org.apache.hudi").option("hoodie.datasource.query.type", "incremental")
                . option("hoodie.datasource.read.begin.instanttime", 0)
                .load("file:///D:/sparksetup/sparkdata/employee_table").show();


    }
}

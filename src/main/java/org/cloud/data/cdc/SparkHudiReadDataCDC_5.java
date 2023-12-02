package org.cloud.data.cdc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkHudiReadDataCDC_5 {
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
        Dataset trips_df_committime = spark.read().format("org.apache.hudi").load("file:///D:/sparksetup/sparkdata/trips_table_cdc");
        trips_df_committime.createOrReplaceTempView("trips_table_cdc_committime");
        spark.sql("select distinct(_hoodie_commit_time) as commitTime from trips_table_cdc_committime order by commitTime desc").show();

        //Get the last commit timestamp
        String[] _hoodie_commit_time = spark.sql("select distinct(_hoodie_commit_time) as commitTime from trips_table_cdc_committime order by commitTime desc")
                .collectAsList().stream().map(row->row.getString(0)).toArray(String[]::new);

        for(int i=0;i<_hoodie_commit_time.length;i++)
            System.out.println("index "+i +" "+_hoodie_commit_time[i]);

        /*
index 0 20231125135300435 --> Add karimnagar record
index 1 20231125132235873 --> Add hyderabad record
index 2 20231125132159084 --> update and delete
index 3 20231125132126085 --> insert
        */

        //Read table data
        Dataset trips_df = spark.read().format("org.apache.hudi")
                .option("hoodie.datasource.read.begin.instanttime",0)
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.query.incremental.format","cdc")
                .load("file:///D:/sparksetup/sparkdata/trips_table_cdc");
        trips_df.createOrReplaceTempView("trips_table_cdc");
        spark.sql("SELECT * FROM  trips_table_cdc order by ts_ms desc").show(false);

    }
}

package org.cloud.data.incremental;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class SparkHudiAddnewData_4 {
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
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();

        Dataset<Row> dataset =  spark.read().option("header",true).csv("employee_batch3.csv");
        dataset.write().format("org.apache.hudi").option("hoodie.table.name","employee_table")
                .option("hoodie.datasource.write.precombine.field", "ts").option("hoodie.datasource.write.recordkey.field", "employee_id")
//                .option("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name())
                .option("hoodie.datasource.write.operation","upsert").mode(SaveMode.Append)
                .save("file:///D:/sparksetup/sparkdata/employee_table");

    }
}

package org.cloud.data.misc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;


public class Ignore_SparkExamplefordataframeupdate {
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
        Dataset employee_df = spark.read().format("org.apache.hudi")
                .load("file:///D:/sparksetup/sparkdata/employee_table");

        Dataset employee_df_updates =  employee_df.filter(employee_df.col("employee_id").equalTo("1"))
                .withColumn("employee_name", functions.lit("Naresh Dulam1.1"));


        //employee_df_updates.show();
        /*
        StructType structType = new StructType();
        structType = structType.add("employee_id", DataTypes.StringType, false);
        structType = structType.add("employee_name", DataTypes.StringType, false);
        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("3","Naresh Dulam3"));
        nums.add(RowFactory.create("1","Naresh Dulam1.1"));
        Dataset<Row> dataset3 = spark.createDataFrame(nums, structType);
        dataset3.show();
        */
        employee_df_updates.write().format("org.apache.hudi").option("hoodie.datasource.write.operation","upsert").option("hoodie.table.name","employee_table")
                .mode(SaveMode.Append).save("file:///D:/sparksetup/sparkdata/employee_table");

    }
}

package org.cloud.data.incremental;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiDeleteData5 {
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
        Dataset employee_df = spark.read().format("org.apache.hudi").load("file:///D:/sparksetup/sparkdata/employee_table");

        Dataset employee_df_updates =  employee_df.filter(employee_df.col("employee_id").equalTo("2"));
        employee_df_updates.write().format("org.apache.hudi").option("hoodie.datasource.write.operation","delete")
//                .option("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name())
                .option("hoodie.datasource.write.precombine.field", "ts").option("hoodie.datasource.write.recordkey.field", "employee_id")
                .option("hoodie.table.name","employee_table").mode(SaveMode.Append).save("file:///D:/sparksetup/sparkdata/employee_table");


    }
}

package org.cloud.data.cdc;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiAddData_4 {
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

        StructType structType = new StructType();
        structType = structType.add("ts", DataTypes.LongType, false);
        structType = structType.add("uuid", DataTypes.StringType, false);
        structType = structType.add("rider", DataTypes.StringType, false);
        structType = structType.add("driver", DataTypes.StringType, false);
        structType = structType.add("fare", DataTypes.DoubleType, false);
        structType = structType.add("city", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1695159649086L,"334e26e9-8355-45cc-97c6-c31daf0d666","rider-G","driver-L",20.0,"hyderabad"));

        Dataset<Row> dataset = spark.createDataFrame(nums, structType);
        dataset.write().format("org.apache.hudi").option(PARTITIONPATH_FIELD_NAME.key(),"city").option("hoodie.table.cdc.enabled",true)
                .option("hoodie.table.name","trips_table_cdc")
                .option("hoodie.table.type", HoodieTableType.COPY_ON_WRITE.name()).mode(SaveMode.Append)
                .save("file:///D:/sparksetup/sparkdata/trips_table_cdc");


    }
}

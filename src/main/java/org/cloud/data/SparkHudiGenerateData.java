package org.cloud.data;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiGenerateData {
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
        //JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Dataset dataset = spark.read().csv("nationalparks.csv");
        //Read table data
        //spark.read().format("org.apache.hudi").load("file:///C:/tmp/spark_shell/spark_warehouse/trips_table").show();

        StructType structType = new StructType();
        structType = structType.add("ts", DataTypes.LongType, false);
        structType = structType.add("uuid", DataTypes.StringType, false);
        structType = structType.add("rider", DataTypes.StringType, false);
        structType = structType.add("driver", DataTypes.StringType, false);
        structType = structType.add("fare", DataTypes.DoubleType, false);
        structType = structType.add("city", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"));
        nums.add(RowFactory.create(1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"));
        nums.add(RowFactory.create(1695046462179L,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"));
        nums.add(RowFactory.create(1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo" ));
        nums.add(RowFactory.create(1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai"));

        Dataset<Row> dataset = spark.createDataFrame(nums, structType);
        dataset.write().format("org.apache.hudi").option(PARTITIONPATH_FIELD_NAME.key(),"city").option("hoodie.table.name","trips_table")
                .option("hoodie.table.type",HoodieTableType.MERGE_ON_READ.name()).mode(SaveMode.Overwrite)
                .save("file:///D:/sparksetup/sparkdata/trips_table");
        System.out.println("Number of lines in file = " + dataset.count());
    }
}

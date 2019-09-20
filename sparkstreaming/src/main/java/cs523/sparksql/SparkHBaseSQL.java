package cs523.sparksql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cs523.sparksql.CustomerReview.HbaseTable;
import scala.Tuple2;

public class SparkHBaseSQL {
	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkHBaseSQL").setMaster("local"));

		// Load our input data
		JavaRDD<String> reviewLines = sc.textFile(args[0]);

		JavaRDD<CustomerReview> reviews = reviewLines.map(CustomerReview::Parser).cache();

		//read from hbase example 
		// JavaPairRDD<String, CustomerReview> records = HbaseTable.ReadFromHbase(sc);
		// System.out.println("begin print record .... ");
		// records.foreach(f->System.out.println(f._1().toString()));
		// System.out.println("end print record .... ");

		//write to hbase example 
		HbaseTable.SaveToHbase(reviews);

		sc.close();
	}

}

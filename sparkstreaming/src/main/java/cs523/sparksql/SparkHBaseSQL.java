package cs523.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import cs523.sparksql.CustomerReview.HbaseTable;

public class SparkHBaseSQL {
	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkHBaseSQL").setMaster("local"));
		sc.setLogLevel("OFF");
		
		//read from hbase example 
		HbaseTable.ReadFromHbase(sc);
		System.out.println("** How do ratings vary with verified_purchase? **");
		DataFrame df1 = HbaseTable.get_verified_purchase();
		df1.show();
		
		System.out.println("How do ratings vary with Vine membership?");
		DataFrame df2 = HbaseTable.get_vine();
		df2.show();
		
		System.out.println("How do ratings vary with Marketplace (i.e. region)?");
		DataFrame df3 = HbaseTable.get_ratings_marketplace();
		df3.show();
		
		System.out.println("How do ratings vary by product category?");
		DataFrame df4 = HbaseTable.get_product_category_rateing();
		df4.show();
		
		System.out.println("How do customers report the helpfulness of different star rating reviews?");
		DataFrame df5 = HbaseTable.get_helpful_rateing();
		df5.show();
		
		System.out.println("Looking at reviewer behaviour...");
		DataFrame df6 = HbaseTable.get_customer_rateing();
		df6.show();
		
		System.out.println("Looking at reviewer behaviour...");
		DataFrame df7 = HbaseTable.get_customer_rateing_verified();
		df7.show();
		
		System.out.println("Which review is the most helpful in Amazon...?");
		DataFrame df8 = HbaseTable.get_helpful_votes();
		df8.show();
		sc.close();
	}

}

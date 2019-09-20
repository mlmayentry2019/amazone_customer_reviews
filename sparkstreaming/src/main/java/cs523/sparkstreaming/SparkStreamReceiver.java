package cs523.sparkstreaming;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cs523.sparksql.CustomerReview;
import cs523.sparksql.CustomerReview.HbaseTable;
import scala.Tuple2;

public class SparkStreamReceiver {

	private static final Pattern TAB = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length < 2) {
	      System.err.println("Usage: SparkStreamReceiver <hostname> <port>");
	      System.exit(1);
	    }
		// Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("SparkStreamReceiver");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
	            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
	    
	    
	    JavaDStream<CustomerReview> reviews = lines.map(CustomerReview::Parser);
	    
	   
	    //JavaPairDStream<String, Integer> wordCounts = reviews.mapToPair(s -> new Tuple2<String, Integer>(s, 1))
	     //   .reduceByKey((i1, i2) -> i1 + i2);

	    System.out.println("start print line ==============");
	    //wordCounts.foreachRDD(e->System.out.println("key = "+e.keys()));
	    System.out.println("end print line ==============");

	    reviews.foreachRDD(rdd ->{
	          if(!rdd.isEmpty()){
	             //rdd.coalesce(1).saveAsTextFile("/user/cloudera/output");
	        	  //rdd.foreach(HbaseTable::SaveToHbase);
	        	  HbaseTable.SaveToHbase(rdd);
	          }
	      });
	    
	   // wordCounts.print();
	    ssc.start();
	    ssc.awaitTermination();
	    ssc.close();
	}

}

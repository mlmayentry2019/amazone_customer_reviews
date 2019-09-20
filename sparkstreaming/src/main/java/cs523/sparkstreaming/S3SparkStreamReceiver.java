package cs523.sparkstreaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import cs523.sparksql.CustomerReview;
import cs523.sparksql.CustomerReview.HbaseTable;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

public class S3SparkStreamReceiver {
  public static void main(String[] args) throws InterruptedException {
    SparkConf sparkConf = new SparkConf().setAppName("s3SparkStream").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    Configuration hadoopConf = sc.hadoopConfiguration();
		hadoopConf.set("fs.s3a.awsAccessKeyId","AKIATPHID53GETTLNQOJ");
    hadoopConf.set("fs.s3a.awsSecretAccessKey","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");
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));

    JavaDStream<String> streamOfRecords = ssc.textFileStream("s3a://amazon-reviews-pds-local/tsv");
    streamOfRecords.print();

    JavaDStream<CustomerReview> reviews = streamOfRecords.map(CustomerReview::Parser);

    reviews.foreachRDD(rdd -> {
      if (!rdd.isEmpty()) {
        HbaseTable.SaveToHbase(rdd);
      }
    });
    

    ssc.start();
    ssc.awaitTermination();
  }
}

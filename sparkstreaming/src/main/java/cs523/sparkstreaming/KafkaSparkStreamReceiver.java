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

import scala.Tuple2;

public class KafkaSparkStreamReceiver {
  public static void main(String[] args) throws InterruptedException {
    SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
    Map<String, String> kafkaParams = new HashMap<String, String>();
    String server = args[0];
    String topic = args[1];
    kafkaParams.put("bootstrap.servers", "kafka:9092");
    kafkaParams.put("group.id", "1");
    Set<String> topicName = Collections.singleton("foo");
    JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils.createDirectStream(ssc, String.class,
        String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicName);

    JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
        .map(new Function<Tuple2<String, String>, String>() {
          private static final long serialVersionUID = 1L;

          public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
          }
        });
    kafkaSparkInputDStream.print();
    ssc.start();
    ssc.awaitTermination();
  }
}
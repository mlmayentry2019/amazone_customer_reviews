package cs523.sparksql;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import scala.Tuple10;

import java.util.ArrayList;

import scala.Tuple2;

public class CustomerReview implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String marketplace;
	private String customer_id;
	private String review_id;
	private String product_id;
	private String product_parent;
	private String product_title;
	private String product_category;
	private String star_rating;
	private String helpful_votes;
	private String total_votes;
	private String vine;
	private String verified_purchase;
	private String review_headline;
	private String review_body;
	private String review_date;
	private static final String DELIMITED = "\t";
	
	private CustomerReview(String marketplace, String customer_id, String review_id, String product_id,
			String product_parent, String product_title, String product_category, String star_rating,
			String helpful_votes, String total_votes, String vine, String verified_purchase, String review_headline,
			String review_body, String review_date) {
		super();
		this.marketplace = marketplace;
		this.customer_id = customer_id;
		this.review_id = review_id;
		this.product_id = product_id;
		this.product_parent = product_parent;
		this.product_title = product_title;
		this.product_category = product_category;
		this.star_rating = star_rating;
		this.helpful_votes = helpful_votes;
		this.total_votes = total_votes;
		this.vine = vine;
		this.verified_purchase = verified_purchase;
		this.review_headline = review_headline;
		this.review_body = review_body;
		this.review_date = review_date;
	}
	public static CustomerReview Parser(String reviewline)
	{
		List<String> review = Arrays.asList(reviewline.split(DELIMITED));
		return new CustomerReview(review.get(0), review.get(1), review.get(2), review.get(3), review.get(4), review.get(5), review.get(6), 
				review.get(7), review.get(8), review.get(9), review.get(10), review.get(11), review.get(12),
				review.get(13), review.get(14));
	}
	public String getMarketplace() {
		return marketplace;
	}
	public String getCustomer_id() {
		return customer_id;
	}
	public String getReview_id() {
		return review_id;
	}
	public String getProduct_id() {
		return product_id;
	}
	public String getProduct_parent() {
		return product_parent;
	}
	public String getProduct_title() {
		return product_title;
	}
	public String getProduct_category() {
		return product_category;
	}
	public String getStar_rating() {
		return star_rating;
	}
	public String getHelpful_votes() {
		return helpful_votes;
	}
	public String getTotal_votes() {
		return total_votes;
	}
	public String getVine() {
		return vine;
	}
	public String getVerified_purchase() {
		return verified_purchase;
	}
	public String getReview_headline() {
		return review_headline;
	}
	public String getReview_body() {
		return review_body;
	}
	public String getReview_date() {
		return review_date;
	}
	public String toString()
	{
		return this.marketplace + DELIMITED +
				this.customer_id  + DELIMITED +
				this.review_id  + DELIMITED +
				this.product_id  + DELIMITED +
				this.product_parent  + DELIMITED +
				this.product_title  + DELIMITED +
				this.product_category  + DELIMITED +
				this.star_rating  + DELIMITED +
				this.helpful_votes  + DELIMITED +
				this.total_votes  + DELIMITED +
				this.vine  + DELIMITED +
				this.verified_purchase  + DELIMITED +
				this.review_headline  + DELIMITED +
				this.review_body  + DELIMITED +
				this.review_date;
	}
	
	public static class HbaseTable
	{
		private static final String TABLE_NAME = "CustomerReview";
		private static final String CF_DEFAULT = "review_data";
		
		private static final String COL_MARKET_PLACE = "marketplace";
		private static final String COL_CUSTUMER_ID = "customer_id";
		private static final String COL_REVIEW_ID = "review_id";
		private static final String COL_PRODUCT_ID = "product_id";
		private static final String COL_PRODUCT_PARENT = "product_parent";
		private static final String COL_PRODUCT_TITLE = "product_title";
		private static final String COL_PRODUCT_CATEGORY = "product_category";
		private static final String COL_STAR_RATE = "star_rating";
		private static final String COL_HELPFUL_VOTES = "helpful_votes";
		private static final String COL_TOTAL_VOTES = "total_votes";
		private static final String COL_VINE = "vine";
		private static final String COL_VERIFIED_PURCHASE = "verified_purchase";
		private static final String COL_REVIEW_HEADLINE = "review_headline";
		private static final String COL_REVIEW_BODY = "review_body";
		private static final String COL_REVIEW_DATE = "review_date";
		
		public static void SaveToHbase(JavaRDD<CustomerReview> reviewRecords) throws IOException
		{
			Configuration config = HBaseConfiguration.create();

			try (Connection connection = ConnectionFactory.createConnection(config);
					Admin admin = connection.getAdmin())
			{
				HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

				
				if (!admin.tableExists(table.getTableName()))
				{
					System.out.print("Creating table.... ");
					admin.createTable(table);
				}
				
				Table reviewTbl = connection.getTable(TableName.valueOf(TABLE_NAME));
				
				for(CustomerReview reviewRecord : reviewRecords.collect())
				{
					System.out.print("Putting data record id: " + reviewRecord.getReview_id()+ "...");
				
					Put put = new Put(Bytes.toBytes(reviewRecord.getReview_id()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_MARKET_PLACE), Bytes.toBytes(reviewRecord.getMarketplace()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_CUSTUMER_ID), Bytes.toBytes(reviewRecord.getCustomer_id()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_ID), Bytes.toBytes(reviewRecord.getReview_id()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_ID), Bytes.toBytes(reviewRecord.getProduct_id()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_PARENT), Bytes.toBytes(reviewRecord.getProduct_parent()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_TITLE), Bytes.toBytes(reviewRecord.getProduct_title()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_CATEGORY), Bytes.toBytes(reviewRecord.getProduct_category()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_STAR_RATE), Bytes.toBytes(reviewRecord.getStar_rating()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_HELPFUL_VOTES), Bytes.toBytes(reviewRecord.getHelpful_votes()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_TOTAL_VOTES), Bytes.toBytes(reviewRecord.getTotal_votes()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VINE), Bytes.toBytes(reviewRecord.getVine()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VERIFIED_PURCHASE), Bytes.toBytes(reviewRecord.getVerified_purchase()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_HEADLINE), Bytes.toBytes(reviewRecord.getReview_headline()));
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_BODY), Bytes.toBytes(reviewRecord.getReview_body()));
			       
					put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_DATE), Bytes.toBytes(reviewRecord.getReview_date()));		
					
					reviewTbl.put(put);
				
				}
			}
		}
		
	}
}

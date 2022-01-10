package com.nycsurge.streaming;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.nycsurge.constants.SurgepricingConstants;
import com.nycsurge.model.CabModel;
import com.nycsurge.utils.JavaSparksessionSingleton;
import com.nycsurge.utils.JavaStreamingContextSingleton;
import com.nycsurge.utils.SurgepricingUtils;
import scala.Tuple2;

public class SurgepricingStreaming implements SurgepricingConstants {
	private static SparkSession sparkSession = null;
	private static JavaStreamingContext streamingContext = null;
	private static final Logger LOG = Logger.getLogger(SurgepricingStreaming.class);

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		streamingContext = JavaStreamingContextSingleton.getInstance(streamingContext, 600000);


	}

	public static void surgePricingCalInStreaming(JavaStreamingContext streamingContext, SparkSession sparkSession) {
		try {
			Set<String> topics = Collections.singleton("supply");
			Map<String, Object> kafkaParams = SurgepricingUtils.getKafkaParams();


			JavaInputDStream<ConsumerRecord<String, String>> messages =
					KafkaUtils.createDirectStream(
							streamingContext,
							LocationStrategies.PreferConsistent(),
							ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));





			messages.foreachRDD(
					ardd -> {
						ardd.foreach(x-> System.out.println(x.topic()));

					}
			);

			streamingContext.start();

			streamingContext.awaitTermination();


/*
			// create consumer
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaParams);

			// subscribe consumer to our topic(s)
			consumer.subscribe(topics);

			// poll for new data
			while(true){
				ConsumerRecords<String, String> records =
						consumer.poll(100); // new in Kafka 2.0.0

				for (ConsumerRecord<String, String> record : records){
					LOG.info("Key: " + record.key() + ", Value: " + record.value());
					LOG.info("Partition: " + record.partition() + ", Offset:" + record.offset());
				}
			}
			*/

//
//			JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
//					streamingContext, LocationStrategies.PreferConsistent(),
//					ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
//
//
//
//
//
//			JavaDStream<String> dstream = directKafkaStream.map(val -> val.value());
//
//			dstream.foreachRDD(rdd -> {
//				JavaRDD<CabModel> rowRdd = rdd.map(msg -> {
//					CabModel mdl = new CabModel(RowFactory.create(msg));
//					System.out.println(msg);
//					return mdl;
//				});
//
//				SparkSession sparkSessionLocal = JavaSparksessionSingleton.getInstance(sparkSession);
//				Dataset<Row> weatherDF = SurgepricingHelperClass.fetchWeatherDataSet(sparkSessionLocal);
//				Dataset<Row> yelloCabDF = sparkSessionLocal.createDataFrame(rowRdd, CabModel.class);
//				Dataset<Row> taxiWithAttrDF = SurgepricingHelperClass.deriveTripAttributes(sparkSessionLocal,
//						yelloCabDF);
//				SurgepricingHelperClass.writeBackSuppyDemandRatio(sparkSessionLocal, taxiWithAttrDF, weatherDF,
//						SURGE_PRICING_ESTIMATION);
//				SurgepricingHelperClass.writeBackTrafficCongestion(sparkSession, taxiWithAttrDF, weatherDF,
//						TRAFFIC_CONGESTION);
//
//			});
//

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			streamingContext.stop();
		}

	}
}

package com.demo.kafka;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread{
	
	private final ConsumerConnector consumer;
	private final String topic;
	
	public KafkaConsumer(String topic) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException{
		//KafkaConsumer consumer=new KafkaConsumer("iis-metrics");
		KafkaConsumer consumer=new KafkaConsumer("logginggw-iislog");
		//KafkaConsumer consumer=new KafkaConsumer("lg-test");
		//KafkaConsumer consumer=new KafkaConsumer("iis-metrics-test");
		consumer.start();
	}
	
	public void run() {
		try {
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()){
				byte[] bytes=it.next().message();
				System.out.println(new String(bytes,"UTF-8"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", 			  "192.168.81.232:2181,192.168.81.231:2181");
		props.put("group.id", 					  "group");
		props.put("auto.offset.reset", 			  "largest");
		props.put("zookeeper.session.timeout.ms", "15000");
		props.put("zookeeper.sync.time.ms", 	  "3000");
		props.put("auto.commit.interval.ms", 	  "1000");
		return new ConsumerConfig(props);
	}
}

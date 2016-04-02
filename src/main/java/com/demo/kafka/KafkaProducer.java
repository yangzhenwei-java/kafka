package com.demo.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.alibaba.fastjson.JSON;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducer {

	private Producer<String, String> producer;
	
	private String metadata_broker_list = "127.0.0.1:9092";
	
	public KafkaProducer(String metadata_broker_list) {
		this.metadata_broker_list = metadata_broker_list;
		producer = new Producer<String, String>(createProducerConfig());
	}
	
	public static void main(String[] args){
		final KafkaProducer producer=new KafkaProducer("192.168.81.232:9092");
		Thread t=new Thread(){

			@Override
			public void run() {
				Random random=new Random(9);
				for(int i=0;i<Integer.MAX_VALUE;i++){
					//producer.send("lg-test",i+"message");
					
					int index=Math.abs(random.nextInt())%10;
					Map<String,Object> params=new HashMap<String,Object>();
					params.put("metricsName","iis.log.datatime_scomputer_cip");
					
					Map<String,Object> metricsItems=new HashMap<String,Object>();
					metricsItems.put("data_time","2015-05-25 15:4"+index);
					metricsItems.put("s_computer","SVR1404HP360");
					metricsItems.put("c_ip","10.8.3.9"+random.nextInt());
					params.put("metricsItems",metricsItems);
					
					Map<String,Object> metricsStatistics=new HashMap<String,Object>();
					metricsStatistics.put("iis.log.sum_column_cs_bytes",25727+index);
					metricsStatistics.put("iis.log.sum_column_sc_bytes",31786+index);
					metricsStatistics.put("iis.log.sum_column_time_token",732+index);
					params.put("metricsStatistics",metricsStatistics);
					
					producer.send("iis-metrics-test",JSON.toJSONString(params));
				}
				System.out.println("producer over");
			}
			
		};
		t.start();
	}

	private ProducerConfig createProducerConfig() {
		Properties properties = new Properties();
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("compression.codec", "none");
		properties.put("queue.buffering.max.ms", "1000");
		properties.put("queue.buffering.max.messages", "30000");
		properties.put("queue.enqueue.timeout.ms", "-1");
		properties.put("request.required.acks", "1");
		properties.put("producer.type", "async");
		properties.put("metadata.broker.list", metadata_broker_list);
		properties.put("metadata.fetch.timeout.ms", 600000L);
		return new ProducerConfig(properties);
	}


	public void send(String topicName, String message) {
		try {
	        if(topicName == null || message == null) {  
	            return;  
	        }
	        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, String.valueOf(Math.random()), message);  
	        producer.send(km);
		} catch (Exception e) {
			shutdown();
    	}
    } 
	
	public void send(String topicName, Collection<?> messages) {
    	try {
    		if(topicName == null || messages == null) {  
                return;  
            }  
            if(messages.isEmpty()){  
                return;
            }
            List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();  
            for(Object entry : messages){
                KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, String.valueOf(Math.random()), JSON.toJSONString(entry));  
                kms.add(km);  
            }  
            producer.send(kms);
    	} catch (Exception e) {
    		shutdown();
    	}
    }
    
    public void shutdown() {
    	if (producer != null) {
    		producer.close();
    		producer = null;
		}
    }

}

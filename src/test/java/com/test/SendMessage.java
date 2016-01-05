package com.test;

import java.util.Properties;
import java.util.Random;

import com.bg.sla.Utils.Constant;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
/**
 * 
 * Description: 检查订单有效性	模拟数据发送
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2015年12月14日 上午10:43:27 
 * @author bai_ge@suixingpay.com
 * 
 *			id	memberid	totalprice	youhui	sendpay	date
 *
	 			4203941	76227	383	57	1	2015-12-14
				6337085	87107	301	25	2	2015-12-14
				9556407	38387	101	77	1	2015-12-14
				9678689	83869	592	72	0	2015-12-14
				7257108	90780	845	18	2	2015-12-14
				4235773	58898	758	49	2	2015-12-14
				6717402	9074	483	73	2	2015-12-14
				6499878	4426	410	96	0	2015-12-14
				523683	72342	772	39	1	2015-12-14
				8548702	7805	935	44	1	2015-12-14
 */
public class SendMessage {
	
	static final String topic = "Topic666";
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("zookeeper.connect", Constant.ZK_HOST_PORT);
		props.put("serializer.class",StringEncoder.class.getName());
		props.put("metadata.broker.list", Constant.KAFKA_BROKER_HOST_PORT);
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String,String> producer = new Producer<String,String>(config);
		Random random = new Random();
		
		for (int i = 0; i < 10000; i++) {
			int id = random.nextInt(10000000);
			int memberid = random.nextInt(100000);
			int totalprice = random.nextInt(1000)+100;
			int youhui = random.nextInt(100);
			int sendpay = random.nextInt(3);
			StringBuffer data = new StringBuffer();
			data.append(String.valueOf(id))
			.append("\t")
			.append(String.valueOf(memberid))
			.append("\t")
			.append(String.valueOf(totalprice))
			.append("\t")
			.append(String.valueOf(youhui))
			.append("\t")
			.append(String.valueOf(sendpay))
			.append("\t")
			.append(String.valueOf("2015-12-14"));
			System.out.println(data.toString());
			producer.send(new KeyedMessage<String, String>(topic, data.toString()));
			Thread.sleep(5000);
			
		}
		producer.close();
		
	}

}

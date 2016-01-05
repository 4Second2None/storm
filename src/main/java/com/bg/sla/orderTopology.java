package com.bg.sla;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bg.sla.Utils.Constant;
import com.bg.sla.bolt.CheckOrderBolt;
import com.bg.sla.bolt.SaveMysqlBolt;
import com.bg.sla.bolt.TranslateBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * 
 * Description: 订单实时校验
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2015年12月14日 下午2:39:42 
 * @author bai_ge@suixingpay.com
 */
public class orderTopology {
	static Logger logger = LoggerFactory.getLogger(orderTopology.class);
	
	static String kafkaZookeeper =null;
	static ZkHosts hosts =null;
	static String topic = null;
	static String zkRoot = null;
	static String id =null;
	static TopologyBuilder builder = null;
	static {
		 hosts = new ZkHosts(Constant.ZK_HOST_PORT);
		 topic = "Topic666";
		 zkRoot = "/order" ;
		 id = UUID.randomUUID().toString();
		 builder = new TopologyBuilder();
	}
	public static void main(String[] args) {
		
		logger.info("********************************************************************");
		logger.info("				  *orderTopology---开始执行...****						");
		logger.info("********************************************************************");
		
		String spout_name = KafkaSpout.class.getSimpleName();
		String checkOrder_name = CheckOrderBolt.class.getSimpleName();
		String trans_name = TranslateBolt.class.getSimpleName();
		String saveMysql_name = SaveMysqlBolt.class.getSimpleName();
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, topic , zkRoot, id);
		builder.setSpout(spout_name, new KafkaSpout(kafkaConfig));
		builder.setBolt(checkOrder_name, new CheckOrderBolt()).shuffleGrouping(spout_name);
		builder.setBolt(trans_name, new TranslateBolt()).shuffleGrouping(checkOrder_name);
		builder.setBolt(saveMysql_name, new SaveMysqlBolt()).shuffleGrouping(trans_name);
		
		Config config = new Config();
		if (args!=null && args.length>0) {
			try {
				StormSubmitter.submitTopology(args[0], config , builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			/**
		     * The maximum parallelism allowed for a component in this topology. This configuration is
		     * typically used in testing to limit the number of threads spawned in local mode.
		     * 
		     * public static final String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";
		     */
			config.setMaxTaskParallelism(3);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("order-topology", config,  builder.createTopology());
			while(true){
				;
			}
//			try {
//				Thread.sleep(500000000);
//			} catch (InterruptedException e) {
//				
//				e.printStackTrace();
//			}
		}
		
	}

}

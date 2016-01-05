package com.bg.sla.kafka2storm;

import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class CountTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		String kafkaZookeeper = "suixingpay194:2181,suixingpay195:2181,suixingpay196:2181,suixingpay197:2181,suixingpay198:2181";
		ZkHosts hosts = new ZkHosts(kafkaZookeeper);
		String topic = "Topic666";
		String zkRoot = "/order" ;
		String id = UUID.randomUUID().toString();
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, topic , zkRoot, id);
		
		String spout_name = KafkaSpout.class.getSimpleName();
		String countbolt_name = CountBolt.class.getSimpleName();
		
		builder.setSpout(spout_name, new KafkaSpout(kafkaConfig));
		builder.setBolt(countbolt_name, new CountBolt()).shuffleGrouping(spout_name);
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
			config.setMaxTaskParallelism(1);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("special-topology", config,  builder.createTopology());
			
			try {
				Thread.sleep(500000000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
}

package com.bbaiggey.storm_01;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
/**
 * 
 * Description: 初识storm----以及storm的 tail特性 只能应用于测试 不能应用于分布式
 * Created on:  2016年9月1日 下午5:53:36 
 * @author bbaiggey
 * @github https://github.com/bbaiggey
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new MySpout(), 1);
		builder.setBolt("bolt", new MyBolt(), 1).shuffleGrouping("spout");
//		builder.setBolt("print", new PrintBolt1(), 1).shuffleGrouping("bolt");

//		Map conf = new HashMap();
//		conf.put(Config.TOPOLOGY_DEBUG, isOn);
//		conf.put(Config.TOPOLOGY_WORKERS, 4);
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		conf.setDebug(true);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
		
		
		

	}

}

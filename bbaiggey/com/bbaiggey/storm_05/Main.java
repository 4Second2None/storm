package com.bbaiggey.storm_05;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.bbaiggey.storm_02.MySpout;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new MySpout(), 1);
		//builder.setBolt("bolt", new PVBolt(), 2).shuffleGrouping("spout");// 计算PV 多线程乘以 线程个数
		builder.setBolt("bolt", new PVBolt1(), 2).shuffleGrouping("spout");
		builder.setBolt("sumBolt", new PVSumBolt(),1).shuffleGrouping("bolt");
		
		Config conf = new Config() ;
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

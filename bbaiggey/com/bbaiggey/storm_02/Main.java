package com.bbaiggey.storm_02;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
/**
 * 
 * Description: 分组方式  shuffleGrouping随机分组  主要用于负载均衡
 * Created on:  2016年9月2日 上午11:07:37 
 * @author bbaiggey
 * @github https://github.com/bbaiggey
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new MySpout(), 1);//并行度 其实就类似于多线程
//		builder.setBolt("bolt", new MyBolt(), 1).shuffleGrouping("spout");//随机分组  负载均衡
		//builder.setBolt("bolt", new MyBolt(), 1).fieldsGrouping("spout", new Fields("log"));// 相同的字段会分到同一个task
		 builder.setBolt("bolt", new MyBolt(), 10).globalGrouping("spout");// 全局分组 会将数据分给最小的task上去执行
//
//		Map conf = new HashMap();
//		conf.put(Config.TOPOLOGY_WORKERS, 4);
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

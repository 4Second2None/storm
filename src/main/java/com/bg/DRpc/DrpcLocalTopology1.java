package com.bg.DRpc;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;




public class DrpcLocalTopology1 {

	/**
	 * 
	 * 
	 * @DESC 不用写Spout因为系统已经内置了 但是由于Bolt中只能有一个execute方法 我们需要些bolt并且通过rpc的映射来实现调用
	 *       “hello”---映射-->execute LinearDRPCTopologyBuilder builder = new
	 *       LinearDRPCTopologyBuilder("hello"); builder.addBolt(new MyBolt());
	 */

	public static class Mybolt extends BaseRichBolt {

		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;

		}

		// 传递过来的tuple包含2个元素，第一个是函数信息，第二个是形参信息
		public void execute(Tuple input) {

			String value = input.getString(1);
			String result = "hello  " + value;
			collector.emit(new Values(input.getValue(0), result));

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("id", "result"));
		}
	}

	public static void main(String[] args) {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"hello");

		builder.addBolt(new Mybolt());
		Config config = new Config();
		// 本地运行
		LocalCluster localCluster = new LocalCluster();
		LocalDRPC drpcServer = new LocalDRPC();

		localCluster.submitTopology(DrpcLocalTopology1.class.getSimpleName(),
				config, builder.createLocalTopology(drpcServer));

		// 客户端调用
		String result = drpcServer.execute("hello", "world");
		System.err.println("客户端调用结果--->" + result);

		localCluster.shutdown();
		drpcServer.shutdown();

	}

}

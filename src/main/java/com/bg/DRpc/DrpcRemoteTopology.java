package com.bg.DRpc;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DrpcRemoteTopology {
	/**
	 * 是需要用户自定义的代码
	 *
	 */
	public static class MyBolt extends BaseRichBolt{
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		//传递过来的tuple包含2个元素，第一个是函数信息，第二个是形参信息
		public void execute(Tuple input) {
			String value = input.getString(1);
			String result = "hello " + value;
			collector.emit(new Values(input.getValue(0), result));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}
		
	}
	
	//storm jar jar.jar  xxxx
	public static void main(String[] args) throws Exception{
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("hello");
		builder.addBolt(new MyBolt());
		
		Config config = new Config();
		
		//远程运行
		StormSubmitter.submitTopology(DrpcRemoteTopology.class.getSimpleName(), config, builder.createRemoteTopology());
	}
}

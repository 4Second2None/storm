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
	 * ����Ҫ�û��Զ���Ĵ���
	 *
	 */
	public static class MyBolt extends BaseRichBolt{
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		//���ݹ�����tuple����2��Ԫ�أ���һ���Ǻ�����Ϣ���ڶ������β���Ϣ
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
		
		//Զ������
		StormSubmitter.submitTopology(DrpcRemoteTopology.class.getSimpleName(), config, builder.createRemoteTopology());
	}
}

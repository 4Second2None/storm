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
	 * @DESC ����дSpout��Ϊϵͳ�Ѿ������� ��������Bolt��ֻ����һ��execute���� ������ҪЩbolt����ͨ��rpc��ӳ����ʵ�ֵ���
	 *       ��hello��---ӳ��-->execute LinearDRPCTopologyBuilder builder = new
	 *       LinearDRPCTopologyBuilder("hello"); builder.addBolt(new MyBolt());
	 */

	public static class Mybolt extends BaseRichBolt {

		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;

		}

		// ���ݹ�����tuple����2��Ԫ�أ���һ���Ǻ�����Ϣ���ڶ������β���Ϣ
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
		// ��������
		LocalCluster localCluster = new LocalCluster();
		LocalDRPC drpcServer = new LocalDRPC();

		localCluster.submitTopology(DrpcLocalTopology1.class.getSimpleName(),
				config, builder.createLocalTopology(drpcServer));

		// �ͻ��˵���
		String result = drpcServer.execute("hello", "world");
		System.err.println("�ͻ��˵��ý��--->" + result);

		localCluster.shutdown();
		drpcServer.shutdown();

	}

}

package com.bg.storm.sum;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * @author bbaiggey
 * @date 2015年4月27日上午10:39:52
 * @DESC 本地模式
 */
public class LoclStormTopology1 {

	public static class DataSourceSpout extends BaseRichSpout {

		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		/**
		 * 只调用一次
		 */
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;

		}

		/**
		 * 不断的循环调用
		 */
		int i = 0;

		public void nextTuple() {

			System.out.println("调用了-->nextTuple     " + i);
			// 送出去，送个bolt
			// Values是一个value的List
			this.collector.emit(new Values(i++));
			Utils.sleep(1000);

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("v1"));

		}

	}

	public static class SumBolt extends BaseRichBolt {

		private Map conf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;

		}

		/**
		 * 死循环，用于接收bolt送来的数据
		 */
		int sum = 0;
		public void execute(Tuple tuple) {

			Integer value = tuple.getIntegerByField("v1");
			sum += value;
			System.out.println(sum);
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}

	}

	/**
	 * 
	 * @param args
	 *            构建拓扑构造器
	 */
	public static void main(String[] args) {
	/**
	 * 创建一个拓扑构造器
	 */
		TopologyBuilder bulider = new TopologyBuilder();
		String spout_name = DataSourceSpout.class.getSimpleName();
		String bolt_nane = SumBolt.class.getSimpleName();
		bulider.setSpout(spout_name, new DataSourceSpout());
		bulider.setBolt(bolt_nane,new SumBolt()).shuffleGrouping(spout_name);
		
		if (args.length==0) {
			LocalCluster localCluster = new LocalCluster();
			Config config = new Config();
			localCluster.submitTopology(LoclStormTopology1.class.getSimpleName(), config, bulider.createTopology());
			Utils.sleep(99999999);
			localCluster.shutdown();
			
		}else {
			
		}
		

	}

}

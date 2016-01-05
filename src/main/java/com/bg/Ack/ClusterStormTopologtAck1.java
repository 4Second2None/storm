package com.bg.Ack;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import scala.util.Random;
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
 * @date 2015年4月27日下午3:52:07
 * @DESC
 */

public class ClusterStormTopologtAck1 {
	
	static AtomicInteger counter = new AtomicInteger(0);//原子操作 不受线程的影响
	
	public static class SourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			this.conf = conf;
			this.context = context;
			
		}
		/**
		 * 认为heartbeat，永无休息，死循环的调用。线程安全的操作。
		 */
		int i = 0;
		public void nextTuple() {

//			System.err.println("Spout  "+ i);
			//送出去，送个bolt
			//Values是一个value的List
			this.collector.emit(new Values(i%2 ,i++), "hello");
			Utils.sleep(1000);
		
			
		}

		//这里的定义是和nextTuple中的值列表对应的  在execute中取值要用到
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			//Fields是一个field的List
			declarer.declare(new Fields("flag", "v1"));
		}
		
		/**
		 * 当后继者bolt发送ack时，这里的ack方法会被调用
		 */
		@Override
		public void ack(Object msgId) {
			super.ack(msgId);
			System.out.println("调用了ack "+msgId);
		}
		
		/**
		 * 当后继者bolt发送fail时，这里的fail方法会被调用
		 */
		@Override
		public void fail(Object msgId) {
			super.fail(msgId);
			System.out.println("调用了fail "+msgId);
		}
		
	}
	
	public static class SumBolt extends BaseRichBolt{
		private Map conf;
		private TopologyContext context;
		private OutputCollector collector;
		
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		
		/**
		 * 死循环，用于接收bolt送来的数据
		 */
		int sum = 0;
		public void execute(Tuple tuple) {
			
			final Integer value = tuple.getIntegerByField("v1");
		
			sum += value;
			System.err.println("线程--->"+Thread.currentThread().getId() + "\t" +  value);
			
			if(value>10 && value<20){
				this.collector.fail(tuple);
			}else{
				this.collector.ack(tuple);
			}
			
			
//			try {
//				value = tuple.getIntegerByField("v1");
//				sum += value;
//				System.err.println(Thread.currentThread().getId() + "\t" +  value);
//				
//				this.collector.ack(tuple);
//			} catch (Exception e) {
//				this.collector.fail(tuple);
//			}
		}


		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			
		}

	}
	

	public static void main(String[] args) {
		/**
		 * 创建一个拓扑构造器
		 */
			TopologyBuilder bulider = new TopologyBuilder();
			String spout_name = SourceSpout.class.getSimpleName();
			String bolt_nane = SumBolt.class.getSimpleName();
			bulider.setSpout(spout_name, new SourceSpout());
			bulider.setBolt(bolt_nane,new SumBolt()).shuffleGrouping(spout_name);
			
			if (args.length==0) {
				LocalCluster localCluster = new LocalCluster();
				Config config = new Config();
				localCluster.submitTopology(ClusterStormTopologtAck1.class.getSimpleName(), config, bulider.createTopology());
				Utils.sleep(99999999);
				localCluster.shutdown();
				
			}else {
				
			}
			

		}
}

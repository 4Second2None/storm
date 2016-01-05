package com.bg.Ack;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
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

/**
 * 
 * @author bbaiggey
 * @date 2015年4月27日下午1:17:59
 * @DESC Storm的安全性  ack  fail 方法
 */
public class ClusterStormTopologyAck 
{
	
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		
		AtomicInteger counter = new AtomicInteger(0);
		final Random random = new Random();
		/**
		 * 在本实例运行时，首先被调用
		 */
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
			
		}
		
		/**
		 * 认为heartbeat，永无休息，死循环的调用。线程安全的操作。
		 */
		int i = 0;
		public void nextTuple() {
			System.err.println("Spout  "+ i);
			//送出去，送个bolt
			//Values是一个value的List
			this.collector.emit(new Values(i%2 ,i++), counter.getAndAdd(1));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


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
//			this.collector.emit(new Values(i%2 ,i++), counter.getAndAdd(1));
			
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
			System.err.println(Thread.currentThread().getId() + "\t" +  value);
			
			if(value>10 && value<20){
				this.collector.fail(tuple);
//				MessageId messageId = tuple.getMessageId();
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
			// TODO Auto-generated method stub
			
		}

	}
	
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException 
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("input", new DataSourceSpout());
        builder.setBolt("sum", new SumBolt(), 3).allGrouping("input");
        
        final Config config = new Config();
		StormSubmitter.submitTopology(ClusterStormTopologyAck.class.getSimpleName(), config, builder.createTopology());
    }
}

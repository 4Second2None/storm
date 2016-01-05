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
 * @date 2015��4��27������1:17:59
 * @DESC Storm�İ�ȫ��  ack  fail ����
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
		 * �ڱ�ʵ������ʱ�����ȱ�����
		 */
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
			
		}
		
		/**
		 * ��Ϊheartbeat��������Ϣ����ѭ���ĵ��á��̰߳�ȫ�Ĳ�����
		 */
		int i = 0;
		public void nextTuple() {
			System.err.println("Spout  "+ i);
			//�ͳ�ȥ���͸�bolt
			//Values��һ��value��List
			this.collector.emit(new Values(i%2 ,i++), counter.getAndAdd(1));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//Fields��һ��field��List
			declarer.declare(new Fields("flag", "v1"));
		}
		
		
		/**
		 * �������bolt����ackʱ�������ack�����ᱻ����
		 */
		@Override
		public void ack(Object msgId) {
			super.ack(msgId);
			System.out.println("������ack "+msgId);
		}
		
		/**
		 * �������bolt����failʱ�������fail�����ᱻ����
		 */
		@Override
		public void fail(Object msgId) {
			super.fail(msgId);
			System.out.println("������fail "+msgId);
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
		 * ��ѭ�������ڽ���bolt����������
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

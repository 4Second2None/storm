package com.bg.Ack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


/**
 * 
 * @author bbaiggey
 * @date 2015��4��27������3:52:07
 * @DESC
 */

public class ClusterStormTopologtAck1 {
	
	static AtomicInteger counter = new AtomicInteger(-1);//ԭ�Ӳ��� �����̵߳�Ӱ��
//	�����̰߳�ȫ��map
	private static ConcurrentHashMap <Integer, Tuple>   fsHashMap = null;
	static ArrayList arrayList = null;
	static  HashMap hashMap = null;
	
	public static class SourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			this.conf = conf;
			this.context = context;
			fsHashMap = new ConcurrentHashMap <Integer, Tuple>();
			 arrayList = new ArrayList();
			  hashMap = new HashMap();
			
		}
		/**
		 * ��Ϊheartbeat��������Ϣ����ѭ���ĵ��á��̰߳�ȫ�Ĳ�����
		 */
		int i = 0;
		public void nextTuple() {

			//�ͳ�ȥ���͸�bolt
			//Values��һ��value��List
//										   flag v1			msgId
			this.collector.emit(new Values(i%2 ,i++), counter.incrementAndGet());
			arrayList.add(counter);
//			fsHashMap.put(counter.get(), 123);
//			hashMap.put(new Random().nextInt(10), new Random().nextInt(10));
			Utils.sleep(500);
		
			
		}

		//����Ķ����Ǻ�nextTuple�е�ֵ�б��Ӧ��  ��execute��ȡֵҪ�õ�
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
			System.out.println("������ack "+msgId+"hashMap.size()---->"+fsHashMap.size());
			fsHashMap.remove(msgId);
		}
		
		/**
		 * �������bolt����failʱ�������fail�����ᱻ����		ʧ�ܿ��Խ����ط�
		 */
		@Override
		public void fail(Object msgId) {
			
			collector.emit(new Values(i%2,fsHashMap.get(msgId).getIntegerByField("v1")+100),msgId);
			System.out.println("������fail "+msgId);
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
			if(value>10 && value<20){
				fsHashMap.put(counter.get(), tuple);
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
		 * ����һ�����˹�����
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

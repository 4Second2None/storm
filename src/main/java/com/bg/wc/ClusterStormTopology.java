package com.bg.wc;

import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
 * ��ҵ��ʵ�ֵ��ʼ�����
 *     (1)Ҫ���һ���ļ����а������ļ�����ȡ�����������ļ��еĵ��ʳ��ִ�����
 *     (2)���ļ����е��ļ����������ǣ�ʵʱ���������ļ��еĵ��ʳ��ִ�����
 *     �ύ����Ⱥ����: storm jar java/test.jar com.bg.wc.ClusterStormTopology
 *     ֹͣ storm kill toplogyName
 *     �鿴 storm list
 */
public class ClusterStormTopology 
{
	
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		
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
			this.collector.emit(new Values(i++));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//Fields��һ��field��List
			declarer.declare(new Fields("v1"));
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
			System.err.println(sum);
		}


		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}

	}
	
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException 
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new DataSourceSpout());
        builder.setBolt("2", new SumBolt()).shuffleGrouping("1");
       
        final Config config = new Config();
        config.setNumAckers(0);
		StormSubmitter.submitTopology(ClusterStormTopology.class.getSimpleName(), config, builder.createTopology());
    }
}

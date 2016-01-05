package com.bg.Group;

import java.util.Map;
import java.util.Random;

import com.bg.Ack.ClusterStormTopologtAck1;

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
import backtype.storm.utils.Utils;

/**
 * 
 * @author Administrator
 *
 */
public class ClusterStormTopologyAllGrouping 
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
			this.collector.emit(new Values(i%2 ,i++));
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
			System.err.println("�߳�ID--��"+Thread.currentThread().getId() + "\t" +  value);
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
        if (args.length==0) {
        	LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(ClusterStormTopologtAck1.class.getSimpleName(), config, builder.createTopology());
			Utils.sleep(99999999);
			localCluster.shutdown();
		}else {
			
			StormSubmitter.submitTopology(ClusterStormTopologyAllGrouping.class.getSimpleName(), config, builder.createTopology());
		}
    }
}

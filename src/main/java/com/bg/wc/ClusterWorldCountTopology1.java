package com.bg.wc;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.Static;

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
 * @author bbaiggey
 * @date 2015��4��27������10:39:52
 * @DESC ����ģʽ
 */
public class ClusterWorldCountTopology1 {
	
	static String INPUTPATH="";
	static String SUFFIX="";

	public static class DataSourceSpout extends BaseRichSpout {

		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		/**
		 * ֻ����һ��
		 */
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;

		}

		/**
		 * ���ϵ�ѭ������
		 */
		int i = 0;

		public void nextTuple() {
			// ��ȡ�ļ��б� ���˺�׺ �Ƿ�ݹ�
			Collection<File> listFiles = FileUtils.listFiles(new File(
					INPUTPATH), new String[] { SUFFIX }, true);
			// ��ÿ���ļ��е�ÿһ�н�������
			for (File file : listFiles) {
				try {

					List<String> lines = FileUtils.readLines(file);
					// ��ÿһ�з����ȥ
					for (String line : lines) {
						this.collector.emit(new Values(line));
					}
					// ��ֹ�ظ�ͳ�� ���ļ�����
					FileUtils.moveFile(file, new File(file.getAbsolutePath()
							+ "." + System.currentTimeMillis()));

				} catch (IOException e) {

					e.printStackTrace();
				}

			}

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("line"));

		}

	}
/**
 * 
 * @author bbaiggey
 * @date 2015��4��27������11:24:20
 * @DESC ��ֵ���
 */
	public static class SplitBolt extends BaseRichBolt {

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
		 * ��ѭ�������ڽ���bolt����������
		 */
		public void execute(Tuple tuple) {

			//��ȡtuple
			String line = tuple.getStringByField("line");
			System.out.println(line+"-------------------");
			//���ÿһ�У��õ�һ��������
			String[] words = line.split("\\s");
			//�ѵ��ʷ����ȥ
			for (String word : words) {
				this.collector.emit(new Values(word));
			}
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

	}

	public static class CountBolt extends BaseRichBolt{
		private Map conf;
		private TopologyContext context;
		private OutputCollector collector;
		
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		
		/**
		 * �Ե��ʽ��м���
		 */
		
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		public void execute(Tuple tuple) {
			//��ȡtuple
			String word = tuple.getStringByField("word");
			//����ÿ������
			Integer value = countMap.get(word);
			if(value==null){
				value = 0;
			}
			value++;
			countMap.put(word, value);
			//�ѽ��д��ȥ
			System.err.println("============================================");
			Utils.sleep(2000);
			for (Entry<String, Integer> entry : countMap.entrySet()) {
				System.out.println(entry);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			
		}

	}
	/**
	 * 
	 * @param args
	 *   �������˹�����
	 */
	public static void main(String[] args) {
	/**
	 * ����һ�����˹�����
	 */
		TopologyBuilder bulider = new TopologyBuilder();
		String spout_name = DataSourceSpout.class.getSimpleName();
		String split_nane = SplitBolt.class.getSimpleName();
		String count_name = CountBolt.class.getSimpleName();
		bulider.setSpout(spout_name, new DataSourceSpout());
		bulider.setBolt(split_nane,new SplitBolt()).shuffleGrouping(spout_name);
		bulider.setBolt(count_name, new CountBolt()).shuffleGrouping(split_nane);
		
		if (args.length==0) {
			LocalCluster localCluster = new LocalCluster();
			Config config = new Config();
			localCluster.submitTopology(ClusterWorldCountTopology1.class.getSimpleName(), config, bulider.createTopology());
			Utils.sleep(99999999);
			localCluster.shutdown();
			
		}else {
			Config conf = new Config();
			conf.put(INPUTPATH, args[0]);
	        conf.put(SUFFIX,  args[1]);
	        conf.setDebug(false);
			
			try {
				StormSubmitter.submitTopology(ClusterStormTopology.class.getSimpleName(), conf , bulider.createTopology());
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			
		}
		

	}

}

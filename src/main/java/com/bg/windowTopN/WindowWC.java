package com.bg.windowTopN;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import clojure.main;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.config__init;
import backtype.storm.tuple__init;
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
 * Description: 滑动窗口TOPN排名
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2016年2月23日 下午4:24:59 
 * @author bai_ge@suixingpay.com
 */
/**
 * 作业：实现单词计数。
 *     (1)要求从一个文件夹中把所有文件都读取，计算所有文件中的单词出现次数。
 *     (2)当文件夹中的文件数量增加是，实时计算所有文件中的单词出现次数。
 *     INPUTPATH	config传参
 *     FILESUFFIX	
 */
public class WindowWC {
	public static class DataSourceSport extends BaseRichSpout {

		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		/**
		 * 在本实例运行时，首先被调用   做初始化的一些操作
		 */
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
			
		}

		/**
		 * 认为heartbeat，永无休息，死循环的调用。线程安全的操作。
		 */
		@Override
		public void nextTuple() {
			File directory = new File("D://test");
			Collection<File> listFiles = FileUtils.listFiles(directory, new String[]{"txt"} , true);
			for (File file : listFiles) {
				try {
					List<String> readLines = FileUtils.readLines(file);
					for (String line : readLines) {
						this.collector.emit(new Values(line));
					}
					//修改后缀名防止重复计算
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+"."+System.currentTimeMillis()));
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
		
	}
	public static class SplitBolt extends BaseRichBolt{
		private Map conf;
		private TopologyContext context;
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.conf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		/**
		 * 
		 *拆分每一行单词
		 */
		@Override
		public void execute(Tuple tuple) {
			//根据定义的字段读取tuple
			String line = tuple.getStringByField("line");
			//拆分没一行，得到一个个单词
			String[] words = line.split("\\s");
			//把单词发射出去
			for (String word : words) {
				this.collector.emit(new Values(word));
			}
		}

		@Override
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
		 * 对单词进行计数
		 */
		
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		public void execute(Tuple tuple) {
			//读取tuple
			String word = tuple.getStringByField("word");
			//保存每个单词
			Integer value = countMap.get(word);
			if(value==null){
				value = 0;
			}
			value++;
			countMap.put(word, value);
			//把结果写出去
			System.err.println("============================================");
			Utils.sleep(2000);
			for (Entry<String, Integer> entry : countMap.entrySet()) {
				System.out.println(entry);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			
		}

	}
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		String DATASOURCESPORT = "sport";
		String SPLIT_BOLT = "splitbolt";
		String COUNT_BOLT = "countbolt";
		
		builder.setSpout(DATASOURCESPORT, new DataSourceSport(),5);
		builder.setBolt(SPLIT_BOLT, new SplitBolt()).shuffleGrouping(DATASOURCESPORT);
		builder.setBolt(COUNT_BOLT, new CountBolt()).shuffleGrouping(SPLIT_BOLT);
		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		localCluster.submitTopology(WindowWC.class.getSimpleName(), config, builder.createTopology());
		Utils.sleep(99999999);
		localCluster.shutdown();
	}
	

}

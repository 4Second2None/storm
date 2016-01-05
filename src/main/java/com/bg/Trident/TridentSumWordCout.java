package com.bg.Trident;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author bbaiggey
 * @date 2015年5月23日下午11:07:14
 * @DESC 自定义数据源 IBatchSpout  实现单词计数
 */
public class TridentSumWordCout {
	
	public static class DataSourceSpout implements IBatchSpout{
		 
		HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

		@Override
		public void open(Map conf, TopologyContext context) {
			
		}

		int sum =0;
		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			Collection<File> listFiles = FileUtils.listFiles(new File("D:\\test"), new String[]{"txt"}, false);
			for (File file : listFiles) {
				try {
					List<String> readLines = FileUtils.readLines(file);
					for (String word : readLines) {
						collector.emit(new Values(word));
					}
//					记得改名 不然重复读取
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void ack(long batchId) {
			 this.batches.remove(batchId);			
		}

		@Override
		public void close() {
			
		}

		@Override
		public Map getComponentConfiguration() {
			Config config = new Config();
			return config;
		}

		/**
		 * 类似于 declareOutputFields
		 */
		@Override
		public Fields getOutputFields() {
			// TODO Auto-generated method stub
			return new Fields("sentence");
		}
		
		
		
	}
	
	
	/**
	 * 
	 * @DESC 类似于bolt  SplitBolt
	 */
	public static class SplitBolt extends BaseFunction {
		int sum =0;
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
		
			String line = tuple.getString(0);
			String[] split = line.split("\t");
			for (String word : split) {
				collector.emit(new Values(word));
			}
			
		}
		
	}
	/**
	 * 
	 * @DESC对单词进行计数   原来是通过创建一个map 进行比较
	 */
	public static class SumBolt extends BaseFunction {
		
		
		HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String word = tuple.getString(0);
			countMap.put(word, MapUtils.getInteger(countMap, word, 0)+1);
			System.out.println("****************************************");
				
			for (Entry<String, Integer> entity : countMap.entrySet()) {
				
				System.err.println(entity);
			}
			
		}
		
	}
	
	public static void main(String[] args) {

		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("sport1", new DataSourceSpout())
		.each(new Fields("sentence"), new SplitBolt(), new Fields("word"))
		.each(new Fields("word"), new SumBolt(),new Fields(""));
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("TridentFunction", new Config(), tridentTopology.build());
		
	}

}

package com.bbaiggey.storm_04;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * Description:   进行汇总操作的 可以使用先多线程局部汇总 在用单线程全局汇总   ---->wordcount 还可以使用 安装字段分组 全部多线程汇总  原理是相同的单词只会出现在一个task中
 * Created on:  2016年9月5日 下午2:14:05 
 * @author bbaiggey
 * @github https://github.com/bbaiggey
 */
public class WordCountTopology {
	/**
	 * 
	 * Description: 继承BaseBasicBolt [IBasicBolt]会自动执行ack 
	 * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
	 * Created on:  2016年9月5日 下午2:15:52 
	 * @author bbaiggey
	 * @github https://github.com/bbaiggey
	 */
	public static class WordCount extends BaseBasicBolt {
		//单词计数
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
		      Integer count = counts.get(word);
		      if (count == null)
		        count = 0;
		      count++;
		      counts.put(word, count);
		     //System.err.println(Thread.currentThread().getName()+"     word="+word+"; count="+count);
		      collector.emit(new Values(word, count));
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
			declarer.declare(new Fields("word","count"));
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		//拓扑构建器
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("MyRandomSentenceSpout", new MyRandomSentenceSpout());
		builder.setBolt("split", new MySplit(" "),10).shuffleGrouping("MyRandomSentenceSpout");
		builder.setBolt("WordCount", new WordCount(),5).fieldsGrouping("split", new Fields("word"));//字段分组多线程也没问题 原因是同一个单词只会在一个task中进行聚合
		//builder.setBolt("WordCount", new WordCount(),5).shuffleGrouping("split"); //单线程没问题  多线程就出问题了 在进行一次汇总结果是正确的
		
		builder.setBolt("finalWordCount", new SumCount(),1).shuffleGrouping("WordCount");
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(4);
		if (args.length>0) {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			
		}else {
			
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("MyWordCount", config, builder.createTopology());
		}
	}

}

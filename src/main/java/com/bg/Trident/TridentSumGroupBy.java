package com.bg.Trident;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.text.html.parser.Entity;

import org.apache.commons.collections.MapUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author bbaiggey
 * @date 2015年5月23日下午11:07:14
 * @DESC groupBy AggeryGet   Aggregator
 */
public class TridentSumGroupBy {
	/**
	 * BaseAggregator
	 */
	public static class MyAgg extends BaseAggregator<Map<String, Integer>> {

		@Override
		public Map<String, Integer> init(Object batchId,
				TridentCollector collector) {
			
			return new HashMap<String, Integer>();
		}

		@Override
		public void aggregate(Map<String, Integer> val, TridentTuple tuple,
				TridentCollector collector) {
			String word = tuple.getString(0);
			val.put(word, MapUtils.getInteger(val, word, 0)+1);
			
		}

		@Override
		public void complete(Map<String, Integer> val,
				TridentCollector collector) {
			collector.emit(new Values(val));
			
		}
	
		
	}
	
	
	public static class MyBolt extends BaseFunction  {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {

			Map<String, Integer> value = (Map<String, Integer>) tuple.getValue(0);
			for (Entry<String, Integer> entity : value.entrySet()) {
				System.err.println(entity);
				
			}
		}
		
	}
	
	
	public static void main(String[] args) {
		 FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 4, new Values("a"),new Values("b"),new Values("a"),new Values("c"));
//		 true 循环调用
		 spout.setCycle(false);
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("sport1", spout)
		.groupBy(new Fields("sentence"))
		.aggregate(new Fields("sentence"), new MyAgg(), new Fields("map"))
		.each(new Fields("map"), new MyBolt(), new Fields(""));
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("TridentFunction", new Config(), tridentTopology.build());
		
	}

}

package com.bg.Trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author bbaiggey
 * @date 2015年5月23日下午11:40:33
 * @DESC 连接 多个stream进行连接到一块
 */
public class TridentMeger {
	/**
	 * 
	 * @DESC 类似于bolt
	 */
	public static class SumBolt extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer sentence = tuple.getInteger(0);
			System.err.println(sentence);

		}

	}

	// 过滤器
	public static class MyFilter extends BaseFilter {

		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer sentence = tuple.getInteger(0);
			return sentence % 2 == 0 ? true : false;
		}

	}

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1,
				new Values(1), new Values(2), new Values(3), new Values(4));
		// true 循环调用
		spout.setCycle(false);
		TridentTopology tridentTopology = new TridentTopology();
		Stream stream = tridentTopology.newStream("sport1", spout);
//		merge(Stream... streams)  可以合并多个stream
		tridentTopology.merge(stream)
		.each(new Fields("sentence"), new MyFilter())
		.each(new Fields("sentence"), new SumBolt(), new Fields(""));
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("TridentFunction", new Config(),
				tridentTopology.build());

	}

}

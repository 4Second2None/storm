package com.bg.Trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author bbaiggey
 * @date 2015��5��23������11:07:14
 * @DESC ����
 */
public class TridentFunction {
	/**
	 * 
	 * @DESC ������bolt
	 */
	public static class SumBolt extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence =  (String) tuple.getValue(0);
			System.err.println(sentence);
			
		}
		
	}
	
	public static void main(String[] args) {
		 FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1, new Values("1"));
//		 true ѭ������
		 spout.setCycle(true);
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("sport1", spout)
		.each(new Fields("sentence"), new SumBolt(),new Fields(""));
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("TridentFunction", new Config(), tridentTopology.build());
		
	}

}

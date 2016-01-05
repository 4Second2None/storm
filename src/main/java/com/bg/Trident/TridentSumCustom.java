package com.bg.Trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author bbaiggey
 * @date 2015��5��23������11:07:14
 * @DESC �Զ�������Դ IBatchSpout  ʵ�� Sum�����ۼ�
 */
public class TridentSumCustom {
	
	public static class MySpout implements IBatchSpout{
		 
		HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

		@Override
		public void open(Map conf, TopologyContext context) {
			
		}

		int sum =0;
		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			
			List<List<Object>> batch = this.batches.get(batchId);
	        if(batch == null){
	            batch = new ArrayList<List<Object>>();
	          System.out.println("------>"+sum);
	                batch.add(new Values(sum++));
	                
	            this.batches.put(batchId, batch);
	        }
	        for(List<Object> list : batch){
	            collector.emit(list);
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
		 * ������ declareOutputFields
		 */
		@Override
		public Fields getOutputFields() {
			// TODO Auto-generated method stub
			return new Fields("sentence");
		}
		
		
		
	}
	
	
	/**
	 * 
	 * @DESC ������bolt
	 */
	public static class SumBolt extends BaseFunction {
		int sum =0;
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
		Integer integer = tuple.getInteger(0);
		
			System.out.println(sum+=integer);
			
		}
		
	}
	
	public static void main(String[] args) {

		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("sport1", new MySpout())
		.each(new Fields("sentence"), new SumBolt(),new Fields(""));
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("TridentFunction", new Config(), tridentTopology.build());
		
	}

}

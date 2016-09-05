package com.bbaiggey.storm_04;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SumCount implements IRichBolt {

	Map counts = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}

	@Override
	public void execute(Tuple tuple) {
		long word_sum = 0;// 总数
		long word_count = 0; // 个数，去重后
		String word = tuple.getString(0);
		Integer count = tuple.getInteger(1);
		counts.put(word, count);
		Iterator<Integer> it = counts.values().iterator();
		while (it.hasNext()) {
			
				word_sum += it.next();
			
			
		}
		
		Iterator<String> it2 = counts.keySet().iterator();
		while (it2.hasNext()) {
			String oneWordString = it2.next();
			if (oneWordString != null) {
				word_count ++ ;
			}
			
		}
	System.out.println("word_sum----> "+word_sum+"\t"+"word_count----->"+word_count);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

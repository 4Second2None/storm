package com.bbaiggey.storm_03;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class finalWordCount extends BaseBasicBolt{

	
	private static final long serialVersionUID = 1L;
	//单词计数
	Map<String, Integer> counts = new HashMap<String, Integer>();
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		  String word = tuple.getString(0);
		  Integer _count = tuple.getInteger(1);
	      Integer count = counts.get(word);
	      if (count == null)
	        count = 0;
	    
	      counts.put(word, count+_count);
	      System.err.println(Thread.currentThread().getName()+"     word="+word+"; count="+counts.get(word));
	      collector.emit(new Values(word, count));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		
	}

}

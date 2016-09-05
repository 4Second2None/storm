package com.bbaiggey.storm_01;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyBolt1 implements IRichBolt{

	private Map<String,Integer> map;
	private OutputCollector collector ; 
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.map = new HashMap<String,Integer>();
		this.collector = collector ;
	}

	@Override
	public void execute(Tuple input) {
		String string = input.getStringByField("word");
		if (map.get(string) != null) {
			map.put(string, map.get(string)+ 1);
		}else {
			map.put(string, 1);
		}
		collector.emit(new Values(map));
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("map"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

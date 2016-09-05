package com.bbaiggey.storm_05;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PVBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	String logString = null;
	
	String session_id = null;
	
	long Pv = 0;
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		logString = input.getString(0);
		session_id = logString.split("\t")[1];
		if (session_id != null) {
			Pv ++ ;
		}
		
		System.err.println("pv = "+ Pv * 2);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
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

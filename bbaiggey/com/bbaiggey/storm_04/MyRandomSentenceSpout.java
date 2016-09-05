package com.bbaiggey.storm_04;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
/**
 * 
 * Description: 数据产生
 * Created on:  2016年9月5日 下午2:26:17 
 * @author bbaiggey
 * @github https://github.com/bbaiggey
 */
public class MyRandomSentenceSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	 SpoutOutputCollector _collector;
	 Random _rand;


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	    _rand = new Random();
		
	}
	String[] sentences = new String[]{ "a a", "b b c c c", "d d e e f f f f"};

	@Override
	public void nextTuple() {
		
		 for (String sentence : sentences) {
		    	_collector.emit(new Values(sentence));
			}
		   Utils.sleep(10*10000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("word"));
		
	}
	
}

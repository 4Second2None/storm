package com.bg.sla.bolt;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * Description: 处理维度数据 
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2015年12月14日 下午6:07:39 
 * @author bai_ge@suixingpay.com
 */
public class TranslateBolt extends BaseBasicBolt {

	
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
//		System.out.println("TranslateBolt.execute()");
		List<Object> list = tuple.getValues();
		String id = (String) list.get(0);
		String memberid = (String) list.get(1);
		String totalprice = (String) list.get(2);
		String youhui = (String) list.get(3);
		String sendpay = (String) list.get(4);
//		CASE WHEN SUBSTRING(sendpay,9,1)="1" THEN 1 WHEN SUBSTRING(sendpay,9,1)="2" THEN 2  ELSE -1 END	
		if("0".equals(sendpay)){
			sendpay = "-1";
		}
		collector.emit(new Values(id,memberid,totalprice,youhui,sendpay));
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay"));
		
	}
	public static void main(String[] args) {
		
	}

}

package com.bg.sla.bolt;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.bg.sla.Utils.DateUtils;
/**
 * 
 * Description: 检查定点日期有效性
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2015年12月14日 下午4:06:37 
 * @author bai_ge@suixingpay.com
 */
public class CheckOrderBolt extends BaseBasicBolt{

	
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
//		System.out.println("CheckOrderBolt.execute()");
		
		String data = new String (tuple.getBinaryByField("bytes"));
		//订单号       用户id       原金额        优惠价     标示字段    下单时间
		//id    memberid     totalprice    youhui     sendpay     createdate 
		if (data!=null &&data.length()>0) {
			String[] values = data.split("\t");
			if (values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String youhui = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				if (StringUtils.isNotEmpty(id)&& StringUtils.isNotEmpty(memberid) && StringUtils.isNotEmpty(totalprice)) {
					if (DateUtils.isDate(createdate, "2015-12-14")) {
						collector.emit(new Values(id,memberid,totalprice,youhui,sendpay));
					}
				}
			}
			
			
			
		}
		
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay"));
		
	}
	/**
	 * 
	 * Description: 测试使用
	 * @author bai_ge@suixingpay.com
	 * @param  @param args
	 * @return void
	 * @throws
	 */
	public static void main(String[] args) {
		String data = "4264782	41157	331	42	2	2015-12-14";
		if(data!=null && data.length()>0){
			String[] values = data.split("\t");
			if(values.length==6){
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String youhui = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				
				if(StringUtils.isNotEmpty(id) && StringUtils.isNotEmpty(memberid) && StringUtils.isNotEmpty(totalprice)){
					if(DateUtils.isDate(createdate, "2015-12-14")){
						System.out.println("   true");
					}else{
						System.out.println("   false");
					}
				}
			}
		}
		
	}

}

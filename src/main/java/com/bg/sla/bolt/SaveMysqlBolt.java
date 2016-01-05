package com.bg.sla.bolt;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import scala.annotation.bridge;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.bg.sla.Utils.JDBCUtil;
import com.bg.sla.Utils.LockCuratorSrc;
import com.bg.sla.Utils.RedisUtils;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
/**
 * 
 * Description: 根据维度将计算结构数据定时入库	入库面临的问题 1、数据丢失	2、分布式事务保证数据不重复
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2015年12月14日 下午6:16:09 
 * @author bai_ge@suixingpay.com
 */
public class SaveMysqlBolt extends BaseBasicBolt  {

	
	private static final long serialVersionUID = 1L;
	
	
	RedisUtils redisUtils = null;
	private static Map<String, String> memberMap= null; //key = sendpay	, value =  id_num+","+tp+","+etp+","+counter_member;
	private static boolean isOpen = true;
	private static List<String> cacheList = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		redisUtils = new RedisUtils();
		memberMap = new HashMap<String, String>();
		cacheList = new ArrayList<String>();
//		定时执行数据的入库----------------数据落地   5s执行一次
		Timer timer = new Timer();
		timer.schedule(new SaveMysqlBolt.cacheTimer(), new Date(), 5000);
		
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		List<Object> list = tuple.getValues();
		String memberid = (String) list.get(1);
		String totalprice = (String) list.get(2);
		String youhui = (String) list.get(3);
		String sendpay = (String) list.get(4);
		saveCounterMember(memberid,sendpay,totalprice,youhui);//记录独立用户数
		

	}

	private void saveCounterMember(String memberid, String sendpay,
			String totalprice, String youhui) {
		String r_key = sendpay+"_"+memberid ;
		String v_x = redisUtils.getVal(r_key);
		boolean isHasRedis = false;
		if (StringUtils.isNotEmpty(v_x)) {
			
			isHasRedis = true;
			
		}else {
			if (r_key!=null&&!r_key.equals("")) {
				
				redisUtils.putVal(r_key, "1");
			}
		}
		
		saveMap(sendpay,isHasRedis,totalprice,youhui);
		
	}

	//计算数据 封装成map结构
	private void saveMap(String sendpay, boolean isHasRedis, String totalprice,	String youhui) {
//		map打开时进行存储数据
	if (isOpen) {
			if(checkMap()){
				String value = memberMap.get(sendpay); ////value = count(id),sum(totalPrice),sum(totalPrice - youhui),count(distinct memberid)
				if (value!=null) {
					String[] vals = value.split(",");
					int id_num = Integer.valueOf(vals[0])+1;
					double tp = Double.valueOf(vals[1])+Double.valueOf(totalprice);
					double etp = Double.valueOf(vals[2])+(Double.valueOf(totalprice)-Double.valueOf(youhui));
					int counter_member = Integer.valueOf(vals[3])+(isHasRedis?0:1);
					value =  id_num+","+tp+","+etp+","+counter_member;
					
				}else{
					value =  1+","+totalprice+","+(Double.valueOf(totalprice)-Double.valueOf(youhui))+","+(isHasRedis?0:1);
				}
				
				System.out.println("sendpay = "+sendpay +"  value = "+value);
				memberMap.put(sendpay,value);
			}
		}else {//存放到临时list的中
			
			String value = sendpay+"_"+1+","+totalprice+","+(Double.valueOf(totalprice)-Double.valueOf(youhui))+","+(isHasRedis?0:1);
			cacheList.add(value);
			
		}
		
	}

	private boolean checkMap(){
		if(memberMap == null){
			memberMap = new HashMap<String, String>();
		}
		return true;
	}
	
	class cacheTimer extends TimerTask{

		@Override
		public void run() {
			
			isOpen = false; //关闭向map中写数据  临时存放在list中
			Map<String, String> tmpMap = new HashMap<String, String>();
			tmpMap.putAll(memberMap);
			memberMap = new HashMap<String, String>(); //重新创建一个新的对象
			
			putDataMap(); //将临时的list中的value 进行拆分 之后放入新创建的map中

			isOpen = true; //打开map
			
			saveMysql(tmpMap);
			
		}

		private void putDataMap() {

			if(checkMap()){
				List<String> tmpList = new ArrayList<String>();
				copyList(tmpList);
				for(String str:tmpList){
					String[] arr = str.split("_");
					if(arr.length==2){
						String sendpay = arr[0];
						String old = arr[1];
						String[] olds = old.split(",");
						int nums = Integer.valueOf(olds[0]);
						String totalprice = olds[1];
						String youhui = olds[2];
						int cm = Integer.valueOf(olds[3]);
						
						String value = memberMap.get(sendpay);
						if(value!=null){
							System.err.println("-----------putDataMap()----得到的map中已经存在值-------------------------");
							String[] vals = value.split(",");
							int id_num = Integer.valueOf(vals[0])+nums;
							double tp = Double.valueOf(vals[1])+Double.valueOf(totalprice);
							double etp = Double.valueOf(vals[2])+(Double.valueOf(youhui));
							int counter_member = Integer.valueOf(vals[3])+cm;
							
							value =  id_num+","+tp+","+etp+","+counter_member;
						}else{
							value =  old;
						}
						
						System.out.println("sendpay = "+sendpay +"  value = "+value);
						
						memberMap.put(sendpay,value);
					}
				}
			}
		
			
		}

		private void copyList(List<String> tmpList) {
			int num = cacheList.size();
			for(int i=0;i<num;i++){
				tmpList.add(cacheList.get(i));
			}
			for(int i=0;i<num;i++){
				cacheList.remove(0);
			}
		}

			/**	
			 * mysql入库		分布式操作数据库 面临着分布式事务问题 使用zookeeper 
			 * 分布式锁来控制事务 这里使用了 Netflix开源的一套ZooKeeper客户端框架Curator
			 * new InterProcessMutex(
			 */
		private void saveMysql(Map<String, String> tmpMap) {
//			加锁
			InterProcessMutex lock = new InterProcessMutex(LockCuratorSrc.getCF(), "/lock/mysql");
			System.out.println("------数据落地入库-------"+tmpMap.toString());

			Connection conn = JDBCUtil.getConnectionByJDBC();
			
			try {
//				轮训获取锁 获取锁之后执行while的代码 执行晚break 跳出  最后释放锁
				while (lock.acquire(10, TimeUnit.MINUTES) ) {
					
				
				for(Map.Entry<String, String> entry:tmpMap.entrySet()){
					//id,order_nums,p_total_price,y_total_price,order_members,sendpay
					String key = entry.getKey();
					String value = entry.getValue();
					String[] vals = value.split(",");
					int id_num = Integer.valueOf(vals[0]);
					double tp = Double.valueOf(vals[1]);
					double etp = Double.valueOf(vals[2]);
					int counter_member = Integer.valueOf(vals[3]);
					
					Statement stmt= conn.createStatement();
					
					//select ...
					String sql = "select id,order_nums,p_total_price,y_total_price,order_members from total_order where sendpay='"+key+"'";
					
					ResultSet set = stmt.executeQuery(sql);
					
					int id = 0;
					int order_nums = 0;
					double p_total_price = 0;
					double y_total_price = 0;
					int order_member = 0;
					
					while(set.next()){
						 id = set.getInt(1);
						 order_nums = set.getInt(2);
						 p_total_price = set.getDouble(3);
						 y_total_price = set.getDouble(4);
						 order_member = set.getInt(5);
					}
					
					order_nums += id_num;
					p_total_price += tp;
					y_total_price += etp;
					order_member += counter_member;
					
					StringBuffer sBuffer = new StringBuffer();
					if(id==0){//insert
						sBuffer.append("insert into total_order(order_nums,p_total_price,y_total_price,order_members,sendpay) values(")
						.append(order_nums+","+p_total_price+","+y_total_price+","+order_member+",'"+key+"')");
					}else{//update
						sBuffer.append("update total_order set order_nums="+order_nums)
						.append(",p_total_price="+p_total_price)
						.append(",y_total_price="+y_total_price)
						.append(",order_members="+order_member)
						.append(" where id="+id);
						
					}
					
					System.out.println("sql = "+sBuffer.toString());
					
					stmt= conn.createStatement();
					stmt.executeUpdate(sBuffer.toString());
					conn.commit();
					
					stmt.close();
				}
//				执行完跳出 并且要记得释放锁 否则别的线程永远得不到锁 无法执行下去
				break;
			}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				try {
					if(conn!=null){
						conn.close();
					}
//					释放锁
					lock.release();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}

package com.test;

import java.util.HashMap;
import java.util.Random;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
	HashMap<String , Integer> hashMap = new HashMap<String, Integer>();
	@org.junit.Test
	public void TestLogger(){
	
		Logger logger = LoggerFactory.getLogger(Test.class);
		logger.info("ssssssssssssssss");
	}
	@org.junit.Test
	public void TestHash(){
		
		for (int i = 0; i < 12; i++) {
			
			
			hashMap.put(i+"", i);
		}
		System.out.println(hashMap.size());
	}
	
	
	
	
	
	@org.junit.Test
	public void TestRandom(){
	
		Logger logger = LoggerFactory.getLogger(Test.class);
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {
			logger.info(random.nextInt()+"");
		}
		
	
	}

}

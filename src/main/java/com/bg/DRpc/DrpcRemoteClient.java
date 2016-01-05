package com.bg.DRpc;

import backtype.storm.utils.DRPCClient;

public class DrpcRemoteClient {

	public static void main(String[] args) throws Exception {
//		drpc.port 	3772  storm默认端口就是3772
		DRPCClient client = new DRPCClient("192.168.120.194", 3772);
		String result = client.execute("hello", "baige");
		System.out.println(result);
	}

}

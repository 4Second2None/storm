package com.bg.Rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
/**
 * 
 * @author bbaiggey
 * @date 2015年4月27日下午2:30:50
 * @DESC RPC 获取服务端的代理对象 之后就可以调用服务端的方法了
 */
public class RPCClient {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BizProtocol proxy = RPC.getProxy(BizProtocol.class, 10010, new InetSocketAddress("localhost", 8888), conf);
		String result = proxy.hello("world");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}

}

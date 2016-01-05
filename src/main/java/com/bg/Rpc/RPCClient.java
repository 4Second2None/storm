package com.bg.Rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
/**
 * 
 * @author bbaiggey
 * @date 2015��4��27������2:30:50
 * @DESC RPC ��ȡ����˵Ĵ������ ֮��Ϳ��Ե��÷���˵ķ�����
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

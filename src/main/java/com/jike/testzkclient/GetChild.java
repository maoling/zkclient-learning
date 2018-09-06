package com.jike.testzkclient;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.base.enums.ZkAdress;

public class GetChild {

	public static void main(String[] args) {
		ZkClient zc = new ZkClient(ZkAdress.Zk_URL,10000,10000,new SerializableSerializer());
		System.out.println("conneted ok!");
		
		List<String> cList = zc.getChildren("/zk-latencies");
		
		System.out.println(cList.size());
		
	}
	
}

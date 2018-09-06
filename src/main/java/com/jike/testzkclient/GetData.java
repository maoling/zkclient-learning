package com.jike.testzkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.base.enums.ZkAdress;

public class GetData {

	public static void main(String[] args) {
		ZkClient zc = new ZkClient(ZkAdress.Zk_URL,10000,10000,new SerializableSerializer());
		System.out.println("conneted ok!");
		
		Stat stat = new Stat();
		User u = zc.readData("/jike5",stat);
		System.out.println(u.toString()+" id: " + u.getId() + " name: " + u.getName());
		System.out.println(stat);
		
	}
	
}

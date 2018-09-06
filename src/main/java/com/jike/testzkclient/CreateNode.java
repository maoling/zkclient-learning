package com.jike.testzkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;

import com.base.enums.ZkAdress;

public class CreateNode {

	public static void main(String[] args) {
		ZkClient zc = new ZkClient(ZkAdress.Zk_URL, 10000,10000,new SerializableSerializer());
		System.out.println("conneted ok!");		
		
		/*User u = new User();
		u.setId(1);
		u.setName("test");*/
		String strdata = "strange";
		String path = zc.create("/nay", strdata, CreateMode.PERSISTENT);
		System.out.println("created path:"+path);
	}
	
}

package com.jike.testzkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.base.enums.ZkAdress;

public class WriteData {

	public static void main(String[] args) {
		ZkClient zc = new ZkClient(ZkAdress.Zk_URL,10000,10000);
		System.out.println("conneted ok!");
		
		
		/*User u = new User();
		u.setId(2);
		u.setName("test2");*/
		//zc.writeData("/configplatform/15", "9999", -1);
		Long id = new Long(999988);
		String string = String.valueOf(id.longValue());
		System.out.println("string:" + string);
		zc.writeData("/configplatform/18", string, -1);
		
	}
	
}

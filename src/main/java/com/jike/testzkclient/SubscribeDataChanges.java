package com.jike.testzkclient;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.base.enums.ZkAdress;

public class SubscribeDataChanges {
	
	private static class ZkDataListener implements IZkDataListener{

		public void handleDataChange(String dataPath, Object data)
				throws Exception {
			// TODO Auto-generated method stub
			//
			System.out.println(dataPath+" has changed to:"+ String.valueOf(data));
		}

		public void handleDataDeleted(String dataPath) throws Exception {
			// TODO Auto-generated method stub
			System.out.println(dataPath);
			
		}
	}

	public static void main(String[] args) throws InterruptedException {
		ZkClient zc = new ZkClient(ZkAdress.Zk_URL,10000,10000);//,new BytesPushThroughSerializer()
		System.out.println("conneted ok!");
		
		zc.subscribeDataChanges("/configplatform/18", new ZkDataListener());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
}

package com.jike.testzkclient;

import com.base.enums.ZkAdress;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

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
		//
		//zkCli.sh
		System.out.println("conneted ok!");

		zc.subscribeDataChanges("/configplatform/22", new ZkDataListener());
		//Thread.sleep(Integer.MAX_VALUE);
	}
	
}

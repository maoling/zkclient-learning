package com.base.balance.server;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.data.Stat;

public class DefaultBalanceUpdateProvider implements BalanceUpdateProvider {

	private String serverPath;
	private ZkClient zc;

	public DefaultBalanceUpdateProvider(String serverPath, ZkClient zkClient) {
		this.serverPath = serverPath;
		this.zc = zkClient;
	}

	public boolean addBalance(Integer step) {
		// TODO Auto-generated method stub
		Stat stat = new Stat();
		ServerData sertData;

		while (true) {

			try {
				sertData = zc.readData(this.serverPath, stat);
				sertData.setBalance(sertData.getBalance() + step);
				zc.writeData(this.serverPath, sertData, stat.getVersion());
				return true;
			} catch (ZkBadVersionException e) {
				// ignore
			} catch (Exception e) {
				return false;
			}
		}

	}

	public boolean reduceBalance(Integer step) {
		// TODO Auto-generated method stub
		return false;
	}	
}

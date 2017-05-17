package com.base.balance.server;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

public class DefaultRegistProvider implements RegistProvider {

	/*@Override
	public void register(Object context) throws Exception {
		// TODO Auto-generated method stub
		// 1:path
		// 2:zkClient
		// 3:serverData

		ZooKeeperRegistContext registContext = (ZooKeeperRegistContext) context;
		String path = registContext.getPath();
		ZkClient zc = registContext.getZkClient();

		try {
			zc.createEphemeral(path, registContext.getData());
		} catch (ZkNoNodeException e) {

			String parentDir = path.substring(0, path.lastIndexOf('/'));
			zc.createPersistent(parentDir, true);
			register(registContext);
		}
	}*/

	public void unRegist(Object context) throws Exception {
		// TODO Auto-generated method stub
		return;

	}

	public void register(Object context) throws Exception {
		// TODO Auto-generated method stub
		
		ZooKeeperRegistContext registContext = (ZooKeeperRegistContext) context;
		String path = registContext.getPath();
		ZkClient zc = registContext.getZkClient();

		try {
			zc.createEphemeral(path, registContext.getData());
		} catch (ZkNoNodeException e) {

			String parentDir = path.substring(0, path.lastIndexOf('/'));
			zc.createPersistent(parentDir, true);
			register(registContext);
		}
	}

}

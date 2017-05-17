package com.base.subscribe;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import com.alibaba.fastjson.JSON;

public class ManageServer {

	private String serversPath;
	private String commandPath;
	private String configPath;
	private ZkClient zkClient;
	private ServerConfig config;
	//用于监听servers子节点变化的
	private IZkChildListener childListener;
	//用于监听commd节点变化的
	private IZkDataListener dataListener;
	private List<String> workServerList;

	public ManageServer(String serversPath, String commandPath,
			String configPath, ZkClient zkClient, ServerConfig config) {
		this.serversPath = serversPath;
		this.commandPath = commandPath;
		this.zkClient = zkClient;
		this.config = config;
		this.configPath = configPath;
		
		childListener = new IZkChildListener() {
			
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				// TODO Auto-generated method stub
				workServerList = currentChilds;
				System.out.print("new work server list has changed! new list is:");
				execList();
			}
		};
		dataListener = new IZkDataListener() {
			
			public void handleDataDeleted(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
			}
			
			public void handleDataChange(String arg0, Object data) throws Exception {
				// TODO Auto-generated method stub
				String cmd = new String((byte[])data);
				System.out.println("new cmd is: " + cmd);
				exeCmd(cmd);
			}
		};
		
	}

	private void initRunning() {
		zkClient.subscribeDataChanges(commandPath, dataListener);
		zkClient.subscribeChildChanges(serversPath, childListener);
	}

	/*
	 * 1: list 2: create 3: modify
	 */
	private void exeCmd(String cmdType) {		
		if ("list".equals(cmdType)) {
			execList();
		} else if ("create".equals(cmdType)) {
			execCreate();
		} else if ("modify".equals(cmdType)) {
			execModify();
		} else {
			System.out.println("error command!" + cmdType);
		}
	}

	private void execList() {
		System.out.println(workServerList.toString());
	}

	private void execCreate() {
		if(!zkClient.exists(configPath)){
			try {
				zkClient.createPersistent(configPath,JSON.toJSONString(config).getBytes());
			}catch (ZkNoNodeException e) {
				// TODO Auto-generated catch block
				String parentDir = configPath.substring(0,configPath.lastIndexOf('/'));
				zkClient.createPersistent(parentDir, true);
				
				execCreate();
			}catch (ZkNodeExistsException e) {
				// TODO Auto-generated catch block
				zkClient.writeData(configPath, JSON.toJSONString(config).getBytes());
			} 
		}
	}

	private void execModify() {
		config.setDbUser(config.getDbUser() + "_modify");
		
		try {
			zkClient.writeData(configPath,JSON.toJSONString(config).getBytes());
		} catch (ZkNoNodeException e) {
			execCreate();
		}
	}

	public void start() {
		initRunning();
	}

	public void stop() {
		zkClient.unsubscribeDataChanges(commandPath, dataListener);
		zkClient.unsubscribeChildChanges(serversPath, childListener);
	}
}

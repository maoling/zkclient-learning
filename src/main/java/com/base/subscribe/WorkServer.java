package com.base.subscribe;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import com.alibaba.fastjson.JSON;


public class WorkServer {
	private ZkClient zkClient;
	private String configPath;
	private String serversPath;
	private com.base.subscribe.ServerData serverData;
	private com.base.subscribe.ServerConfig serverConfig;
	private IZkDataListener dataListener;
	
	public WorkServer(String configPath,String serversPath,
			         ServerData serverData,ZkClient zkClient,
			         ServerConfig initConfig){
		this.zkClient = zkClient;
		this.serversPath = serversPath;
		this.configPath = configPath;
		this.serverConfig = initConfig;
		this.serverData = serverData;
		
		dataListener = new IZkDataListener() {
			
			public void handleDataDeleted(String arg0) throws Exception {
				// TODO Auto-generated method stub				
			}
			
			public void handleDataChange(String arg0, Object data) throws Exception {
				// TODO Auto-generated method stub
				String retJson = new String((byte[])data);
				ServerConfig serverConfigLocal = JSON.parseObject(retJson,ServerConfig.class);
			    updateConfig(serverConfigLocal);
			    System.out.println("new server worker config is: " + serverConfig);
			}
		};
		
	}
	public void start(){
		System.out.println("work server has started!");
		initRunning();
		
	}
    public void stop(){
    	System.out.println("work server has stoped!");
    	zkClient.unsubscribeDataChanges(configPath, dataListener);
	}
    private void initRunning(){
    	register();
    	//订阅Config节点的改变
    	zkClient.subscribeDataChanges(configPath, dataListener);
    }
    private void register(){
    	String path = serversPath.concat("/").concat(serverData.getAddress());
    	try {
			zkClient.createEphemeral(path, JSON.toJSONString(serverData).getBytes());
		} catch (ZkNoNodeException e) {
			// TODO Auto-generated catch block
			zkClient.createPersistent(serversPath,true);
			register();
		} 
    	
    	
    }
    private void updateConfig(ServerConfig serverConfigLocal){
    	this.serverConfig = serverConfigLocal;
    }
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

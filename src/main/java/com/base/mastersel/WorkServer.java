package com.base.mastersel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;

public class WorkServer {
    private volatile boolean running = false;
    private ZkClient zkClient;
	private static final String MASTER_PATH = "/master";
	//用于监听Master节点的删除事件
	private IZkDataListener dataListener;
	//记录当前服务器的基本信息
	private RunningData serverData;
	//记录当前master节点的基本信息
	private RunningData masterData;
	private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
	private int delayTime = 5;//发现上次的leader不是自己，就延迟五秒钟，给可能由于网络抖动，已经被删除节点的上次master节点的优先再次成为master的机会
	
	public WorkServer(RunningData runningData){
		this.serverData = runningData;
		this.dataListener = new IZkDataListener() {
			//事件监听函数
			public void handleDataDeleted(String arg0) throws Exception {
				// TODO Auto-generated method stub
				//takeMaster();
				//针对网络抖动的优化策略：
				if(masterData != null && masterData.getName().equals(serverData.getName())){
					takeMaster();
				}else{
					delayExector.schedule(new Runnable() {						
						public void run() {
							// TODO Auto-generated method stub
							takeMaster();
						}
					}, delayTime, TimeUnit.SECONDS);
				}
			}
			
			public void handleDataChange(String arg0, Object arg1) throws Exception {
				// TODO Auto-generated method stub
				
			}
		};
	}
	public void start() throws Exception{
		if(running){
			throw new Exception("server has been alearly startup ...");
		}
		running = true;
		zkClient.subscribeDataChanges(MASTER_PATH, dataListener);
		takeMaster();
	}
	public void stop() throws Exception{
		if(!running){
			throw new Exception("server has stoped ...");
		}
		running = false;
		zkClient.unsubscribeDataChanges(MASTER_PATH, dataListener);
		releaseMaster();
	}
	private void takeMaster(){
		if(!running) return;
		try {
			zkClient.create(MASTER_PATH, serverData, CreateMode.EPHEMERAL);//ephemeral,创建临时节点
		    masterData = serverData;
		    System.out.println(serverData.getName()+" is master");
		    
		    //测试，每隔五秒钟释放master
		    delayExector.schedule(new Runnable() {				
				public void run() {
					if(checkMaster()){
						releaseMaster();
					}					
				}
			}, delayTime,TimeUnit.SECONDS);
		} catch (ZkNodeExistsException e) {
			//读取当前master的基本信息放入masterData之中
			RunningData runningData = zkClient.readData(MASTER_PATH, true);
			//如果没有读取到，说明在这一瞬间master宕机了
			if(runningData == null){
				takeMaster();//此时有机会争取master
			}else{
				masterData = runningData;
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
    private void releaseMaster(){
		if(checkMaster()){
			zkClient.delete(MASTER_PATH);
		}
	}
    private boolean checkMaster(){
		try {
			RunningData eventData = zkClient.readData(MASTER_PATH);
			masterData = eventData;
			if(masterData.getName().equals(serverData.getName())){
				return true;
			}
			return false;
		}catch (ZkNoNodeException e) {
			return false;
		}catch (ZkInterruptedException e) {
			return checkMaster();
		}catch (ZkException e) {
			e.printStackTrace();
		}
		return false;
	}
    public void setZkClient(ZkClient client) {
		// TODO Auto-generated method stub
		this.zkClient = client;
	}
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> a = new ArrayList<String>();
		a.add("2");
		a.add("1");
		for (String temp : a) {
			if ("1".equals(temp)) {
				a.remove(temp);
			}
		}
		System.out.println(a.toString());
	}
}

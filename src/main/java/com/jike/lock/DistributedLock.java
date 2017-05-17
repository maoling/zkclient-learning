package com.jike.lock;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {
	
	/*
	 * 获取锁，如果没有得到就等待
	 */
	public void acquire() throws Exception;

	/*
	 * 获取锁，直到超时
	 */
    public boolean acquire(long time, TimeUnit unit) throws Exception;

	/*
	 * 释放锁
	 */
    public void release() throws Exception;


}

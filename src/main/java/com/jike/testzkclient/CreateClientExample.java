package com.jike.testzkclient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class CreateClientExample {
	private static final String PATH = "/configplatform/22";

	public static void main(String[] args) throws Exception {
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		try {
			client = createSimple(zkConnString);
			client.start();
//			client.create().creatingParentsIfNeeded()
//					.forPath(PATH, "test".getBytes());

			/**
			 * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
			 */
			ExecutorService pool = Executors.newFixedThreadPool(2);

			/**
			 * 监听数据节点的变化情况
			 */
			final NodeCache nodeCache = new NodeCache(client, PATH, false);
			nodeCache.start(true);
            CountDownLatch countDownLatch = new CountDownLatch(1);

			nodeCache.getListenable().addListener(
					new NodeCacheListener() {
						@Override
						public void nodeChanged() throws Exception {
							System.out.println("Node data is changed, new data: "
                                    + " path:" + nodeCache.getPath() + " "
									+ new String(nodeCache.getCurrentData().getData()));
						}
					},
					pool
			);
			countDownLatch.await();
            //Thread.sleep(Integer.MAX_VALUE);
//			CloseableUtils.closeQuietly(client);
//
//			client = createWithOptions(zkConnString,
//					new ExponentialBackoffRetry(1000, 3), 1000, 1000);
//			client.start();
//			System.out.println(new String(client.getData().forPath(PATH)));
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
		}

	}

	public static CuratorFramework createSimple(String connectionString) {
		// these are reasonable arguments for the ExponentialBackoffRetry.
		// The first retry will wait 1 second - the second will wait up to 2
		// seconds - the
		// third will wait up to 4 seconds.
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000,
				3);
		// The simplest way to get a CuratorFramework instance. This will use
		// default values.
		// The only required arguments are the connection string and the retry
		// policy
		return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	}

	public static CuratorFramework createWithOptions(String connectionString,
													 RetryPolicy retryPolicy, int connectionTimeoutMs,
													 int sessionTimeoutMs) {
		// using the CuratorFrameworkFactory.builder() gives fine grained
		// control
		// over creation options. See the CuratorFrameworkFactory.Builder
		// javadoc details
		return CuratorFrameworkFactory.builder()
				.connectString(connectionString).retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs)
				.sessionTimeoutMs(sessionTimeoutMs)
				// etc. etc.
				.build();
	}
}

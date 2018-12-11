/*
package com.base.curator;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.base.enums.ZkAdress;

public class DistributedBarrierExample {
	private static final int QTY = 10;
	private static final String PATH = "/examples/barrier";

	public static void main(String[] args) throws Exception {
		*/
/*do {
			System.out.println("ooxxooxx");
		} while (false);*//*


        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(ZkAdress.Zk_URL, new ExponentialBackoffRetry(1000, 3));
            client.start();
            ExecutorService service = Executors.newFixedThreadPool(QTY);
            for (int i = 0; i < QTY; ++i) {
                final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, PATH, QTY);
                final int index = i;
                Callable<Void> task = () -> {

                    Thread.sleep((long) (3 * Math.random()));
                    System.out.println("Client #" + index + " enters");
                    barrier.enter();
                    System.out.println("Client #" + index + " begins");
                    Thread.sleep((long) (3000 * Math.random()));
                    //barrier.leave();
                    System.out.println("Client #" + index + " left");
                    return null;
                };
                service.submit(task);
            }

            service.shutdown();
            service.awaitTermination(3, TimeUnit.MINUTES);
        } catch (Exception e) {
			e.printStackTrace();
		}
    }

}*/

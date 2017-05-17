
package com.jike.balance.server;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;


public class ServerHandler extends ChannelHandlerAdapter{

	
	private final BalanceUpdateProvider balanceUpdater;
	private static final Integer BALANCE_STEP = 1; 

    
    public ServerHandler(BalanceUpdateProvider balanceUpdater){
    	this.balanceUpdater = balanceUpdater;
    	
    } 

    public BalanceUpdateProvider getBalanceUpdater() {
		return balanceUpdater;
	}	
	
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    	System.out.println("one client connect...");
    	balanceUpdater.addBalance(BALANCE_STEP);
    }
	
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    	balanceUpdater.reduceBalance(BALANCE_STEP);
    }

	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }


}

/*
 * Copyright 2020 Guo Chaosheng
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nopasserby.fastminimq;

import static org.nopasserby.fastminimq.MQConstants.MQBroker.SERVER_DECODE_MAX_FRAME_LENGTH;

import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MQServer implements Runnable {
    
    private static Logger logger = LoggerFactory.getLogger(MQServer.class);
    
    private SocketAddress inetAddress;
    
    private MQExecutor mqExecutor;
    
    private NioEventLoopGroup parentGroup;
    
    private NioEventLoopGroup childGroup;
    
    public MQServer(SocketAddress inetAddress, MQExecutor mqExecutor) {
        this.inetAddress = inetAddress;
        this.mqExecutor = mqExecutor;
    }

    @Override
    public void run() {
        try {
            ServerBootstrap bootstrap = createServer();
            
            configureProcessingPipeline(bootstrap);
            configureTCPIPSettings(bootstrap);
            startServer(bootstrap);
            
        } catch (InterruptedException e) {
            logger.error("server error", e);
        }
    }
    
    public void shutdown() {
        parentGroup.shutdownGracefully();
        childGroup.shutdownGracefully();
    }
    
    protected ServerBootstrap createServer() {
        parentGroup = new NioEventLoopGroup();
        childGroup = new NioEventLoopGroup();
       
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(parentGroup, childGroup);
        return bootstrap;
    }

    protected void startServer(ServerBootstrap bootstrap) throws InterruptedException {
        Channel ch = bootstrap.bind(inetAddress).sync().channel();
        logger.info("server started and listen on {}", ch.localAddress());
        ch.closeFuture().sync();
    }

    protected void configureTCPIPSettings(ServerBootstrap bootstrap) {
        bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
    }

    protected void configureProcessingPipeline(ServerBootstrap bootstrap) {
        bootstrap.channel(NioServerSocketChannel.class).childHandler(mqExecutor.channelHandler(SERVER_DECODE_MAX_FRAME_LENGTH));
    }

}

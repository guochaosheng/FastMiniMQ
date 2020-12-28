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

import static org.nopasserby.fastminimq.MQConstants.PARALLEL_CHANNELS;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.CLIENT_DECODE_MAX_FRAME_LENGTH;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class MQClient {
    
    private static Logger logger = LoggerFactory.getLogger(MQClient.class);
    
    private Bootstrap bootstrap;
    
    private MQExecutor mqExecutor;
    
    private NioEventLoopGroup eventGroup;
    
    public MQClient(MQExecutor mqExecutor) {
        this.mqExecutor = mqExecutor;
    }
    
    public Bootstrap configureBootstrap(Bootstrap bootstrap) {
        configureProcessingPipeline(bootstrap);
        configureTCPIPSettings(bootstrap);
        return bootstrap;
    }
    
    protected void configureTCPIPSettings(Bootstrap bootstrap) {
    }

    protected void configureProcessingPipeline(Bootstrap bootstrap) {
        bootstrap.channel(NioSocketChannel.class).handler(mqExecutor.channelHandler(CLIENT_DECODE_MAX_FRAME_LENGTH));
    }

    protected Bootstrap createClient() {
        eventGroup = new NioEventLoopGroup();
        return new Bootstrap().group(eventGroup);
    }

    public MQSender createMQSender(SocketAddress socketAddress) {
        return new MQSender(socketAddress, PARALLEL_CHANNELS);
    }
    
    public MQSender createMQSender(SocketAddress socketAddress, int channelCount) {
        return new MQSender(socketAddress, channelCount);
    }
    
    synchronized List<Channel> createChannels(SocketAddress socketAddress, int channelCount) throws Exception {
        List<Channel> channels = Collections.synchronizedList(new ArrayList<Channel>());
        for (int i = 0; i < channelCount; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            ChannelFuture cf = bootstrap().connect(socketAddress).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (cf.isSuccess()) channels.add(cf.channel());
                    latch.countDown();
                }
            });
            latch.await();
            if (!cf.isSuccess()) return channels;
        }
        return channels;
    }
    
    private Bootstrap bootstrap() {
        if (bootstrap == null) {
            synchronized (this) {
                if (bootstrap == null) {
                    bootstrap = configureBootstrap(createClient());
                }
            }
        }
        return bootstrap;
    }

    public void shutdown() {
        eventGroup.shutdownGracefully();
    }
    
    public class MQSender {
        
        private final SocketAddress socketAddress;
        
        private int channelCount;
        
        private volatile List<Channel> channels;
        
        private AtomicInteger sequence = new AtomicInteger();
        
        public MQSender(SocketAddress socketAddress, int channelCount) {
            this.socketAddress = socketAddress;
            this.channelCount = channelCount;
            this.channels = new ArrayList<Channel>(channelCount);
        }
        
        public SocketAddress socketAddress() {
            return socketAddress;
        }
        
        public void ensureActive() throws Exception {
            ensureChannels(1); // least one
        }
        
        public void write(ByteBuffer buffer) throws Exception {
            if (channels.size() < this.channelCount) ensureChannels(channelCount);
            
            Channel channel = channels.get(Math.abs(sequence.incrementAndGet()) % channels.size());
            
            int retry = this.channelCount;
            while (!channel.isWritable() && retry > 0) {
                channel = channels.get(Math.abs(sequence.incrementAndGet()) % channels.size());
                if (!channel.isActive()) ensureChannels(channelCount);
                retry--;
            }
            
            ByteBuf out = Unpooled.copiedBuffer(buffer);
            
            boolean writable = channel.isWritable();
            Object lock = new Object();
            ChannelFuture cf = channel.writeAndFlush(out);
            cf.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) logger.error(future.channel().toString() + " error in write operation", future.cause());
                    
                    if (writable) return;
                    
                    synchronized (lock) {
                        lock.notify();
                    }
                }
            });
            
            if (writable) return;
            
            synchronized (lock) {
                lock.wait(10);
            }
        }
        
        public void close() {
            for (Channel channel: channels) {
                channel.close();
            }
        }
        
        void ensureChannels(int channelCount) throws Exception {
            synchronized (this) {
                List<Channel> channels = new ArrayList<Channel>();
                for (Channel channel: this.channels) {
                    if (!channel.isActive()) {
                        channel.close();
                        continue;
                    }
                    channels.add(channel);
                }
                int activeCount = channels.size();
                if (activeCount < channelCount) {
                    channels.addAll(createChannels(socketAddress, channelCount - activeCount));
                    if (channels.isEmpty()) {
                        throw new ConnectException("channel not found");
                    }
                    this.channels = channels;
                }
            }
        }
        
    }

}

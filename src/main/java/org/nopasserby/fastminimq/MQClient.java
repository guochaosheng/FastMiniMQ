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
import java.util.List;
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
    
    synchronized List<Channel> createChannles(SocketAddress socketAddress, int channelCount) throws Exception {
        List<Channel> channels = new ArrayList<Channel>();
        for (int i = 0; i < channelCount; i++) {
            ChannelFuture cf = bootstrap().connect(socketAddress).sync();
            channels.add(cf.channel());
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
        
        private List<Channel> channels;
        
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
            for (Channel channel: channels) {
                if (channel.isActive()) return;
                channel.close();
            }
            synchronized (this) {
                channels.add(bootstrap().connect(socketAddress).sync().channel());
            }
        }
        
        public void write(ByteBuffer buffer) throws Exception {
            Channel channel = null;
            do {
                ensureChannels();
                synchronized (this) {
                    int selected = Math.abs(sequence.incrementAndGet()) % channels.size();
                    Channel tmpchannel = channels.get(selected);
                    if (!tmpchannel.isActive()) {                
                        tmpchannel.close();
                        channels.remove(selected);   
                        continue;
                    }
                    channel = tmpchannel;
                }
            } while (channel == null);
            
            ByteBuf out = Unpooled.copiedBuffer(buffer);
            
            int retry = channels.size();
            while (!channel.isWritable() && retry > 0) {
                channel = channels.get(Math.abs(sequence.incrementAndGet()) % channels.size());
                retry--;
            }
            
            if (channel.isWritable()) {
                channel.writeAndFlush(out).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            Throwable exception = future.cause();
                            Channel channel = future.channel();
                            logger.error(channel.localAddress() + "->" + channel.remoteAddress() + " error in write operation", exception);
                        }
                    }
                });
            } else {
                try {
                    channel.writeAndFlush(out).sync();
                } catch (InterruptedException exception) {
                    logger.error(channel.localAddress() + "->" + channel.remoteAddress() + " error in write operation", exception);
                }
            }
        }
        
        public void close() {
            for (Channel channel: channels) {
                channel.close();
            }
        }
        
        public boolean hasAvailableChannel() {
            return !channels.isEmpty();
        }
        
        void ensureChannels() throws Exception {
            int availableCount = channels.size();
            if (availableCount < channelCount) {
                synchronized (this) {
                    if (availableCount < channelCount) {
                        int expandCount = channelCount - availableCount;
                        channels.addAll(createChannles(socketAddress, expandCount));
                    }
                    if (!hasAvailableChannel()) {
                        throw new ConnectException("no available channels found");
                    }
                }
            }
        }
        
    }

}

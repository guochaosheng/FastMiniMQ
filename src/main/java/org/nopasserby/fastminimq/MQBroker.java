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

import static java.lang.Integer.toHexString;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_CODE_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_ID_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.CONSUME;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_PUT;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_GET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_DEL;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.PRODUCE;
import static org.nopasserby.fastminimq.MQUtil.startThread;
import static org.nopasserby.fastminimq.MQUtil.toInetAddress;

import java.net.InetSocketAddress;
import org.nopasserby.fastminimq.MQExecutor.MQDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class MQBroker extends MQDispatch implements Runnable {
    
    private static Logger logger = LoggerFactory.getLogger(MQBroker.class);
    
    private MQStorage storage;
    
    private MQServer server;
    
    private MQKVdb kvdb;
    
    private ConsumeQueue consumeQueue;
    
    private ConsumeProcessor consumeProcessor; 
    
    private MQBrokerCfg brokerCfg;
    
    public MQBroker(MQBrokerCfg brokerCfg) throws Exception {
        this.brokerCfg = brokerCfg;
        this.server = new MQServer(brokerCfg.socketAddress(), this);
        this.kvdb = new MQKVdb();
        this.storage = new MQStorage(kvdb);
        this.consumeQueue = new ConsumeQueue(storage);
        this.consumeProcessor = new ConsumeProcessor(consumeQueue);
    }
    
    @Override
    protected void execute(ChannelDelegate channel, ByteBuf commandWrapper) throws Exception {
        int commandCode = commandWrapper.getInt(COMMAND_CODE_OFFSET);
        long commandId = commandWrapper.getLong(COMMAND_ID_OFFSET);
        try {
            super.execute(channel, commandWrapper);
        } catch (Exception e) {
            logger.error(name() + " command " + toHexString(commandCode) + " execute error", e);
            
            channel.writeAndFlush(buildErr(commandCode, commandId, e));
        }
    }

    @Override
    protected void exceptionCaught(ChannelDelegate channel, Throwable cause) throws Exception {
        logger.error("channel {} error", channel, cause);
    }
    
    public String name() {
        return brokerCfg.name();
    }
    
    @Override
    protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        switch (commandCode) {
            case PRODUCE: {
                storage.dispatch(channel, commandCode, commandId, commandData); 
                break;
            }
            case CONSUME: {
                consumeProcessor.dispatch(channel, commandCode, commandId, commandData); 
                break;
            }
            case KV_PUT: {
                storage.dispatch(channel, commandCode, commandId, commandData); 
                break;
            }
            case KV_GET: {
                kvdb.dispatch(channel, commandCode, commandId, commandData); 
                break;
            }
            case KV_DEL: {
                kvdb.dispatch(channel, commandCode, commandId, commandData); 
                break;
            }
            default: throw new IllegalArgumentException("command[code:" + Integer.toHexString(commandCode) + "] not support.");
        }
    }
    
    @Override
    public void run() {
        MQUtil.envLog(logger, name());
        startThread(storage, "MQ-BROKER-STORAGE");
        startThread(consumeQueue, "MQ-BROKER-CONSUMEQUEUE");
        startThread(server, "MQ-BROKER-SERVER");
    }
    
    public void shutdown() throws Exception {
        server.shutdown();
        storage.shutdown();
        consumeQueue.shutdown();
    }
    
    public static class MQBrokerCfg {
        
        private String clusterName;
        
        private String name;
        
        private String address;
        
        public MQBrokerCfg(String name, String address) {
            this.name = name;
            this.address = address;
        }
        
        public MQBrokerCfg(String clusterName, String name, String address) {
            this(name, address);
            this.clusterName = clusterName;
        }
        
        public String clusterName() {
            return clusterName;
        }
        
        public String name() {
            return name;
        }
        
        public InetSocketAddress socketAddress() {
            return toInetAddress(address);
        }
        
    }

}

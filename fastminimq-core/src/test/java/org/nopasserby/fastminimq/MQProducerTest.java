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

import static org.nopasserby.fastminimq.MQConstants.MQCommand.PRODUCE;
import static org.nopasserby.fastminimq.MQUtil.throwableToString;
import org.nopasserby.fastminimq.MQProducer.MQFutureMetaData;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.rmi.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQExecutor.MQDispatch;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MQProducerTest {

	@Test
	public void sendMsgTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic); 
		MQFuture<MQRecord> future = producer.sendMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		
		Assert.assertEquals(Status.OK, record.getStatus());
	}
	
	@Test
	public void sendTxMsgTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic); 
		MQFuture<MQRecord> future = producer.sendTxMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		
		Assert.assertEquals(Status.OK, record.getStatus());
	}
	
	@Test
	public void commitTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic);
		MQFuture<MQRecord> future = producer.sendTxMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		
		MQFuture<MQRecord> newFuture = producer.commit(record);
		MQRecord newRecord = newFuture.get();
		
		Assert.assertEquals(Status.OK, newRecord.getStatus());
	}
	
	@Test
	public void rollbackTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic); 
		MQFuture<MQRecord> future = producer.sendTxMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		
		MQFuture<MQRecord> newFuture = producer.rollback(record);
		MQRecord newRecord = newFuture.get();
		Assert.assertEquals(Status.OK, newRecord.getStatus());
	}
	
	@Test
	public void remoteFailTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic, -1);
		MQFuture<MQRecord> future = producer.sendMsg(topic, "hello world!".getBytes());
		long id = ((MQFutureMetaData) future).recordMetaData.futureId;
		Exception err = new IllegalAccessException("record cannot be processed");
		MQDispatch dispatch = new MQDispatch() {
			@Override
			protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {}
		};
		producer.dispatch(null, PRODUCE, id , Unpooled.buffer().writeInt(Status.FAIL.ordinal()).writeBytes( dispatch.encodeException(err)));
		MQRecord record = future.get();
		Assert.assertEquals(Status.FAIL, record.getStatus());
		Assert.assertEquals(throwableToString(err), record.getException().getMessage());
	}
	
	@Test
	public void retryTest() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic, 3);
		MQFuture<MQRecord> future = producer.sendMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		Assert.assertEquals(Status.OK, record.getStatus());
		Assert.assertEquals(2, ((MQFutureMetaData) future).recordMetaData.retry);
	}
	
	@Test
	public void retryTest2() throws Exception {
		String topic = "testTopic";
		MQProducer producer = createTestProducer(topic, 4);
		MQFuture<MQRecord> future = producer.sendMsg(topic, "hello world!".getBytes());
		MQRecord record = future.get();
		Assert.assertEquals(Status.FAIL, record.getStatus());
		Assert.assertEquals(3, ((MQFutureMetaData) future).recordMetaData.retry);
	}
	
	public MQProducer createTestProducer(String topic) throws Exception {
		return createTestProducer(topic, 0);
	}
	
	public MQProducer createTestProducer(String topic, int retry) throws Exception {
		String clusterName = "cluster-1";
		String producerName = "producer-1";
		MQProducerCfg producerCfg = new MQProducerCfg(producerName, clusterName);
		
		MQClusterMetaData clusterMetaData = new MQClusterMetaData(clusterName);
		int nodeCount = 4;
		for (int i = 0; i < nodeCount; i++) {
    	    String brokerName = "broker-" + i;
    	    String brokerAddress = "127.0.0.1:600" + i;
    	    clusterMetaData.addBrokerMetaData(new MQBrokerMetaData(brokerName, brokerAddress));
		}
		
		AtomicInteger retryCount = new AtomicInteger();
		
		MQProducer producer = new MQProducer(producerCfg, new MQClusterMetaData(clusterName));
		
		MQDispatch dispatch = new MQDispatch() {
			@Override
			protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
				producer.dispatch(channel, commandCode, commandId, commandData);
			}
		};
		
		producer.metaDataLoad(new MQClient(dispatch) {
			@Override
			public MQSender createMQSender(SocketAddress socketAddress) {
				return new MQSender(socketAddress, 0) {
					@Override
					public void write(ByteBuffer buffer) throws Exception {
						if (retry == -1) {
							return;
						} else if (retryCount.incrementAndGet() < retry) {
							throw new ConnectException("cannot connect");
						} else {			
							ByteBuffer newBuffer = ByteBuffer.allocate(buffer.remaining()).put(buffer);
							newBuffer.flip();
							new Thread() {
								public void run() {
									while (newBuffer.hasRemaining()) {
										int commandCode = newBuffer.getInt();
										long commandId = newBuffer.getLong();
										int commandLength = newBuffer.getInt();
										newBuffer.get(new byte[commandLength]);
										try {
											dispatch.dispatch(null, commandCode, commandId, Unpooled.buffer().writeInt(Status.OK.ordinal()));
										} catch (Exception e) {
											// future.wait
										}
									}
								};
							}.start();
						}
					}
				};
			}
		}, clusterMetaData.metaData());
		
		return producer;
	}
	
}

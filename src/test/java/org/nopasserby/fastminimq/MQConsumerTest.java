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

import static org.nopasserby.fastminimq.MQConstants.NON_TX;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.CLIENT_DECODE_MAX_FRAME_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.nextUUID;
import static org.nopasserby.fastminimq.ReocrdHelper.createReocrd;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQExecutor.MQDispatch;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class MQConsumerTest {

	@Test
	public void fetchMsgTest() throws Exception {
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.OK.ordinal()); // status
		futureCommandData.putLong(100);                // next index
		// record list
		String topic = "testTopic";
		List<String> txidList = new ArrayList<String>();
		List<String> bodyList = new ArrayList<String>();
		int size = 100;
		for (int index = 0; index < size; index++) {
			byte[] txid = nextUUID();
			txidList.add(ByteBufUtil.hexDump(txid));
			String body = "hello world!(" + index + ")";
			bodyList.add(body);
			futureCommandData.put(createReocrd(txid, topic, NON_TX, body.getBytes()));
		}
		futureCommandData.flip();
		//build response commandData end
		
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(queue);
		MQResult<List<MQRecord>> result = future.get();
		List<MQRecord> recordList = result.getResult();
		
		Assert.assertEquals(size, recordList.size());
		
		for (int index = 0; index < size; index++) {
			MQRecord record = recordList.get(index);
			
			Assert.assertEquals(txidList.get(index), ByteBufUtil.hexDump(record.getId()));
			Assert.assertEquals(bodyList.get(index), new String(record.getBody()));
		}
	}
	
	@Test
	public void fetchMsgTest2() throws Exception {
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.FAIL.ordinal()); // status
		Exception err = new IllegalAccessException("fetch record list fail");
		String exception = MQUtil.throwableToString(err);
		futureCommandData.put(exception.getBytes());
		futureCommandData.flip();
		//build response commandData end
		
		String topic = "testTopic";
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(queue);
		MQResult<List<MQRecord>> result = future.get();
		
		Assert.assertEquals(Status.FAIL, result.getStatus());
		Assert.assertEquals(exception, result.getException().getMessage());
	}
	
	@Test
	public void fetchUpdateTest() throws Exception {
		long index = 100;
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.OK.ordinal());
		futureCommandData.putShort((short) Short.BYTES);
		futureCommandData.putLong(index);
		futureCommandData.flip();
		//build response commandData end
		
		String topic = "testTopic";
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		consumer.fetchUpdate(queue);
		
		Assert.assertEquals(index, queue.nextIndex());
	}
	
	@Test(expected = RuntimeException.class)
	public void fetchUpdateTest2() throws Exception {
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.FAIL.ordinal()); // status
		Exception err = new IllegalAccessException("fetch record list fail");
		String exception = MQUtil.throwableToString(err);
		futureCommandData.put(exception.getBytes());
		futureCommandData.flip();
		//build response commandData end
		
		String topic = "testTopic";
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		consumer.fetchUpdate(queue);
	}
	
	@Test
	public void waitAckTest() throws Exception {
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.OK.ordinal());
		futureCommandData.flip();
		//build response commandData end
		
		String topic = "testTopic";
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		consumer.waitAck(queue);
	}
	
	@Test(expected = RuntimeException.class)
	public void waitAckTest2() throws Exception {
		//build response commandData start
		ByteBuffer futureCommandData = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
		futureCommandData.putInt(Status.FAIL.ordinal());
		futureCommandData.flip();
		//build response commandData end
		
		String topic = "testTopic";
		MQConsumer consumer = createTestConsumer(topic, futureCommandData);
		MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
		consumer.waitAck(queue);
	}
	
	public static MQQueue createMQQueue(String topic, MQGroup group) {
		MQQueue queue = new MQQueue();
		MQGroup[] groups = MQGroup.values();
		queue.setGroup(MQGroup.class.getSimpleName());
		queue.setTopic(topic);
		queue.setSubgroups(groups.length);
		queue.setSubgroupNo(group.ordinal());
		queue.setStep(400);
		return queue;
	}
	
	enum MQGroup {
		GROUP_1
	}
	
	public MQConsumer createTestConsumer(String topic, final ByteBuffer futureCommandData) throws Exception {
		String consumerName = "consumer-1";
		String clusterName = "cluster-1";
		String brokerName = "broker-1";
		String brokerAddress = "127.0.0.1:6000";
		MQConsumerCfg consumerCfg = new MQConsumerCfg(consumerName, clusterName, brokerName);
		MQClusterMetaData clusterMetaData = new MQClusterMetaData(clusterName);
		clusterMetaData.addBrokerMetaData(new MQBrokerMetaData(brokerName, brokerAddress));
		
		MQConsumer consumer = new MQConsumer(consumerCfg, new MQClusterMetaData(null));
		
		MQDispatch dispatch = new MQDispatch() {
			@Override
			protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
				consumer.dispatch(channel, commandCode, commandId, commandData);
			}
		};
		
		consumer.metaDataLoad(new MQClient(dispatch) {
			@Override
			public MQSender createMQSender(SocketAddress socketAddress) {
				return new MQSender(socketAddress, 0) {
					@Override
					public void write(ByteBuffer buffer) throws Exception {
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
										dispatch.dispatch(null, commandCode, commandId, Unpooled.wrappedBuffer(futureCommandData));
									} catch (Exception e) {
										// future.wait
									}
								}
							};
						}.start();
					}
				};
			}
		}, clusterMetaData.metaData());

		consumer.start();
		return consumer;
	}
	
}

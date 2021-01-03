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

import static java.io.File.separator;
import static org.nopasserby.fastminimq.MQConstants.LogType.commit;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_RINGBUFFER_BLOCKUNIT;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_BATCH_SIZE;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_PUT;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.PRODUCE;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import static org.nopasserby.fastminimq.ReocrdHelper.createReocrd;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQConstants.Transaction;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class MQStorageTest {

	@Test
	public void dispatchTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
		MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
		String topic = "testTopic";
		long commandId = MQUtil.nextId();
		ByteBuffer commandData = createNonTransactionRecord(topic);
		commandData.getInt(); // skip record length
		
		new Thread(storage).start();
		
		CountDownLatch latch = new CountDownLatch(1);
		storage.dispatch(new ChannelDelegate(null) {
			@Override
			public void write(ByteBuf bytebuf) {
				bytebuf.skipBytes(4);
				Assert.assertEquals(commandId, bytebuf.readLong());
				bytebuf.skipBytes(4);
				Assert.assertEquals(Status.OK.ordinal(), bytebuf.readInt());
				
				latch.countDown();
			}
			@Override
			public void flush() {
			}
		}, PRODUCE, commandId, Unpooled.wrappedBuffer(commandData));
	
		latch.await();
		
		storage.getCommitLog().delete();
		storage.getBuffer().release();
	}
	
	@Test
	public void dispatchFailTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
		MQStorage storage = new MQStorage(kvdb, 1, ringBuffer, commitLog);
		String topic = "testTopic";
		long commandId_1 = MQUtil.nextId();
		long commandId_2 = MQUtil.nextId();
		ByteBuffer commandData = createNonTransactionRecord(topic);
		commandData.getInt(); // skip record length
		
		storage.dispatch(new ChannelDelegate(null), PRODUCE, commandId_1, Unpooled.copiedBuffer(commandData));
		
		storage.dispatch(new ChannelDelegate(null) {
			@Override
			public void writeAndFlush(ByteBuf bytebuf) {
				bytebuf.skipBytes(4);
				Assert.assertEquals(commandId_2, bytebuf.readLong());
				bytebuf.skipBytes(4);
				Assert.assertEquals(Status.FAIL.ordinal(), bytebuf.readInt());
			}
		}, PRODUCE, commandId_2, Unpooled.copiedBuffer(commandData));
		
		storage.getCommitLog().delete();
		storage.getBuffer().release();
	}
	
	@Test
	public void dispatch16KTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
		MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
		String topic = "testTopic";
		long commandId = MQUtil.nextId();
		String seg = " hello world! ";
		String body = seg;
		while (body.getBytes().length < 16 * 1024) {
			body += seg;
		}
		ByteBuffer commandData = createReocrd(MQUtil.nextUUID(), topic, Transaction.NON.ordinal(), body.getBytes());
		
		new Thread(storage).start();
		
		CountDownLatch latch = new CountDownLatch(1);
		storage.dispatch(new ChannelDelegate(null) {
			@Override
			public void write(ByteBuf bytebuf) {
				bytebuf.skipBytes(4);
				Assert.assertEquals(commandId, bytebuf.readLong());
				bytebuf.skipBytes(4);
				Assert.assertEquals(Status.OK.ordinal(), bytebuf.readInt());
				
				latch.countDown();
			}
			@Override
			public void flush() {
			}
		}, PRODUCE, commandId, Unpooled.wrappedBuffer(commandData));
	
		latch.await();
		
		storage.getCommitLog().delete();
		storage.getBuffer().release();
	}
	
	@Test
	public void dispatchOffsetTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
		MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
		MQConsumer consumer = new MQConsumer(null, new MQClusterMetaData(null));
		long index = 1234;
		MQQueue queue = createMQQueue("testTopic", MQGroup.GROUP_1, index);
		ByteBuffer command = consumer.buildAckOut(0, queue);
		
		new Thread(storage).start();
		
		CountDownLatch latch = new CountDownLatch(1);
		storage.dispatch(new ChannelDelegate(null) {
			@Override
			public void write(ByteBuf bytebuf) {
				latch.countDown();
			}
			@Override
			public void flush() {}
		}, KV_PUT, 0, Unpooled.copiedBuffer(command.array(), COMMAND_DATA_OFFSET, command.remaining() - COMMAND_DATA_OFFSET));
		
		latch.await();
		
		ByteBuffer queryData = consumer.buildUpdateOut(0, queue);
		String consumeQueueKey = ByteBufUtil.hexDump(queryData.array(), COMMAND_DATA_OFFSET, queryData.remaining() - COMMAND_DATA_OFFSET);
		byte[] resultIndex = kvdb.getTable().remove(consumeQueueKey);
		
		Assert.assertEquals(index, ByteBuffer.wrap(resultIndex).getLong());
		
		storage.getCommitLog().delete();
		storage.getBuffer().release();
	}
	
	enum MQGroup {
		GROUP_1
	}
	
	public static MQQueue createMQQueue(String topic, MQGroup group, long index) {
		MQQueue queue = new MQQueue();
		MQGroup[] groups = MQGroup.values();
		queue.setTopic(topic);
		queue.setGroup(MQGroup.class.getSimpleName());
		queue.setSubgroups(groups.length);
		queue.setSubgroupNo(group.ordinal());
		queue.setStep(400);
		queue.nextIndex(index);
		return queue;
	}
	
	@Test
	public void dispatchMultithreadingTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
		MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
		String topic = "testTopic";
		ByteBuffer commandData = createNonTransactionRecord(topic);
		commandData.getInt(); // skip record length
		
		new Thread(storage).start();
		
		Object empty = new Object();
		Map<Long, Object> futureMap_1 = new ConcurrentHashMap<Long, Object>();
		Map<Long, Integer> futureMap_2 = new ConcurrentHashMap<Long, Integer>();

		AtomicInteger counter = new AtomicInteger(-1);
		int nthread = 4;
		int batch = 1024 * 128;
		CyclicBarrier barrier = new CyclicBarrier(nthread);
		CountDownLatch latch = new CountDownLatch(nthread);
		ExecutorService executor = Executors.newFixedThreadPool(nthread);
		for (int i = 0; i < nthread; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						barrier.await();
						
						while (counter.incrementAndGet() < batch) {
							long commandId = MQUtil.nextId();
							futureMap_1.put(commandId, empty);
							storage.dispatch(new ChannelDelegate(null) {
								@Override
								public void writeAndFlush(ByteBuf bytebuf) {
									bytebuf.skipBytes(4);
									long commandId = bytebuf.readLong();
									bytebuf.skipBytes(4);
									int status = bytebuf.readInt();
									
									futureMap_2.put(commandId, status);
								}
								public void write(ByteBuf bytebuf) {
									bytebuf.skipBytes(4);
									long commandId = bytebuf.readLong();
									bytebuf.skipBytes(4);
									int status = bytebuf.readInt();
									
									futureMap_2.put(commandId, status);
								};
								public void flush() {};
							}, PRODUCE, commandId, Unpooled.wrappedBuffer(commandData));
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						latch.countDown();
					}
				}
			});
		}
	
		latch.await();
		executor.shutdown();
		
		long lastTimestamp = System.currentTimeMillis();
		while (!futureMap_1.isEmpty() && System.currentTimeMillis() - lastTimestamp < TimeUnit.SECONDS.toMillis(10)) {			
			Set<Long> keyset = futureMap_2.keySet();
			for (Long key: keyset) {
				futureMap_1.remove(key);
			}
		}
		
		Assert.assertEquals(0, futureMap_1.size());
		Assert.assertEquals(batch, futureMap_2.size());
		
		storage.getCommitLog().delete();
		storage.getBuffer().release();
	}
	
}

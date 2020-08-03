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

import io.netty.buffer.ByteBufUtil;
import static java.io.File.separator;
import static org.nopasserby.fastminimq.MQConstants.LogType.consume_subqueue_checkpoint_idx;
import static org.nopasserby.fastminimq.MQConstants.LogType.consume_subqueue_idx;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.ReocrdHelper.createKVRecord;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.ConsumeQueueIndex.GlobalIdIndex;
import org.nopasserby.fastminimq.ConsumeQueueIndex.SubqueueIndex;
import org.nopasserby.fastminimq.ConsumeQueueIndex.SubqueueTopicIndex;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;

public class ConsumeQueueCheckPointTest {

	@Test
	public void createPointTest() throws Exception {
		String topic = "testTopic";
		Integer topicId = 0;
		long subqueueOrder = 0;
		ConsumeQueueCheckPoint consumeQueueCheckPoint = new ConsumeQueueCheckPoint(true);
		SubqueueIndex subqueueIndex = new SubqueueIndex(subqueueOrder);
		SubqueueTopicIndex subqueueTopicIndex_1 = subqueueIndex.createTopicIndex(topicId);
		long offset_1 = 100;
		int length_1 = 100;
		long time_1 = System.currentTimeMillis();
		long offset_2 = 200;
		int length_2 = 200;
		long time_2 = System.currentTimeMillis();
		subqueueTopicIndex_1.writeLong(offset_1);
		subqueueTopicIndex_1.writeInt(length_1);
		subqueueTopicIndex_1.writeLong(time_1);
		subqueueTopicIndex_1.writeLong(offset_2);
		subqueueTopicIndex_1.writeInt(length_2);
		subqueueTopicIndex_1.writeLong(time_2);
		
		GlobalIdIndex globalIdIndex = new GlobalIdIndex();
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()),0L);
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()),0L);
		
		MQKVdb kvdb = new MQKVdb();
		ByteBuffer recordData_1 = createKVRecord(topic, 0);
		kvdb.add(recordData_1);
		ByteBuffer recordData_2 = createKVRecord(topic, 0);
		kvdb.add(recordData_2);
		
		long offset = 1000;
		long index = 400;
		consumeQueueCheckPoint.createPoint(subqueueIndex, globalIdIndex, kvdb, offset, index);
		
		SubqueueTopicIndex subqueueTopicIndex_2 = consumeQueueCheckPoint.reloadIndex(subqueueOrder, topicId);
		ByteBuffer buffer = subqueueTopicIndex_2.buffer();
		
		Assert.assertEquals(offset_1, buffer.getLong());
		Assert.assertEquals(length_1, buffer.getInt());
		Assert.assertEquals(time_1, buffer.getLong());
		Assert.assertEquals(offset_2, buffer.getLong());
		Assert.assertEquals(length_2, buffer.getInt());
		Assert.assertEquals(time_2, buffer.getLong());
		
		consumeQueueCheckPoint.delete();
	}
	
	@Test
	public void reloadTest() throws Exception {
		String topic = "testTopic";
		Integer topicId = 0;
		long subqueueOrder = 0;
		ConsumeQueueCheckPoint consumeQueueCheckPoint = new ConsumeQueueCheckPoint(true);
		SubqueueIndex subqueueIndex = new SubqueueIndex(subqueueOrder);
		SubqueueTopicIndex subqueueTopicIndex_1 = subqueueIndex.createTopicIndex(topicId);
		long offset_1 = 100;
		int length_1 = 100;
		long time_1 = System.currentTimeMillis();
		long offset_2 = 200;
		int length_2 = 200;
		long time_2 = System.currentTimeMillis();
		subqueueTopicIndex_1.writeLong(offset_1);
		subqueueTopicIndex_1.writeInt(length_1);
		subqueueTopicIndex_1.writeLong(time_1);
		subqueueTopicIndex_1.writeLong(offset_2);
		subqueueTopicIndex_1.writeInt(length_2);
		subqueueTopicIndex_1.writeLong(time_2);
		
		GlobalIdIndex globalIdIndex = new GlobalIdIndex();
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()),0L);
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()),0L);
		
		MQKVdb kvdb = new MQKVdb();
		ByteBuffer recordData_1 = createKVRecord(topic, 0);
		kvdb.add(recordData_1);
		ByteBuffer recordData_2 = createKVRecord(topic, 0);
		kvdb.add(recordData_2);
		
		long offset = 1000;
		long index = 400;
		consumeQueueCheckPoint.createPoint(subqueueIndex, globalIdIndex, kvdb, offset, index);
		
		consumeQueueCheckPoint.clear();
		
		consumeQueueCheckPoint.resumeCheckPoint(Long.MAX_VALUE);
		
		SubqueueTopicIndex subqueueTopicIndex_2 = consumeQueueCheckPoint.reloadIndex(subqueueOrder, topicId);
		ByteBuffer buffer = subqueueTopicIndex_2.buffer();
		
		Assert.assertEquals(offset_1, buffer.getLong());
		Assert.assertEquals(length_1, buffer.getInt());
		Assert.assertEquals(time_1, buffer.getLong());
		Assert.assertEquals(offset_2, buffer.getLong());
		Assert.assertEquals(length_2, buffer.getInt());
		Assert.assertEquals(time_2, buffer.getLong());
		
		consumeQueueCheckPoint.delete();
	}
	
	@Test
	public void deleteExpiredCheckPointTest() throws Exception {
		Integer topicId = 0;
		long subqueueOrder = 0;
		ConsumeQueueCheckPoint consumeQueueCheckPoint = new ConsumeQueueCheckPoint(true,
				new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + consume_subqueue_idx, 1024, 128)), 
				new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + consume_subqueue_checkpoint_idx, 1024, 128)));
		// 16 + 20 * 48 = 976
		SubqueueIndex subqueueIndex_1 = new SubqueueIndex(subqueueOrder++);
		SubqueueTopicIndex subqueueTopicIndex_1 = subqueueIndex_1.createTopicIndex(topicId);
		for (int i = 0; i < 48; i++) {			
			subqueueTopicIndex_1.writeLong(100 * i);
			subqueueTopicIndex_1.writeInt(10 * i);
			subqueueTopicIndex_1.writeLong(System.currentTimeMillis());
		}
		
		// (16 + 8) * 2 = 48
		GlobalIdIndex globalIdIndex = new GlobalIdIndex();
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()), 0L);
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()), 100L);
		
		MQKVdb kvdb = new MQKVdb();
		
		long offset_1 = 1000;
		long index_1 = 400;
		consumeQueueCheckPoint.createPoint(subqueueIndex_1, globalIdIndex, kvdb, offset_1, index_1);
		
		SubqueueIndex subqueueIndex_2 = new SubqueueIndex(subqueueOrder++);
		SubqueueTopicIndex subqueueTopicIndex_2 = subqueueIndex_2.createTopicIndex(topicId);
		for (int i = 0; i < 48; i++) {			
			subqueueTopicIndex_2.writeLong(200 * i);
			subqueueTopicIndex_2.writeInt(20 * i);
			subqueueTopicIndex_2.writeLong(System.currentTimeMillis());
		}
		
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()), 1100L);
		globalIdIndex.add(ByteBufUtil.hexDump(MQUtil.nextUUID()), 1200L);
		
		long offset_2 = 2000;
		long index_2 = 800;
		consumeQueueCheckPoint.createPoint(subqueueIndex_2, globalIdIndex, kvdb, offset_2, index_2);
		
		consumeQueueCheckPoint.deleteExpiredCheckPoint(offset_1);
		
		SubqueueTopicIndex subqueueTopicIndex_1_1 = consumeQueueCheckPoint.reloadIndex(0, topicId);
		
		Assert.assertNull(subqueueTopicIndex_1_1);
		
		SubqueueTopicIndex subqueueTopicIndex_2_1 = consumeQueueCheckPoint.reloadIndex(1, topicId);
		
		Assert.assertNotNull(subqueueTopicIndex_2_1);
		
		consumeQueueCheckPoint.delete();
	}
	
}

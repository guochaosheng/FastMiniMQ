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

import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_HEAD_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_BODY_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.DYNAMIC;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import io.netty.buffer.ByteBufUtil;

public class MQKVdbTest {

	@Test
	public void addAndGetTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQConsumer consumer = new MQConsumer(null, new MQClusterMetaData(null));
		long index = 1234;
		MQQueue queue = createMQQueue("testTopic", MQGroup.GROUP_1, index);
		ByteBuffer command = consumer.buildAckOut(0, queue);
		command.position(COMMAND_DATA_OFFSET);
		ByteBuffer recordData = ByteBuffer.allocate(REOCRD_BODY_OFFSET + command.remaining());
		recordData.putInt(REOCRD_HEAD_LENGTH + command.remaining()); // record length
		recordData.putShort(DYNAMIC);                                // record type
		recordData.putLong(MQUtil.currentTimeMillis());              // record time stamp
		recordData.put(command);
		recordData.flip();
		
		kvdb.add(recordData.duplicate());
		kvdb.add(recordData.duplicate());
		
		ByteBuffer queryData = consumer.buildUpdateOut(0, queue);
		String consumeQueueKey = ByteBufUtil.hexDump(queryData.array(), COMMAND_DATA_OFFSET, queryData.remaining() - COMMAND_DATA_OFFSET);
		byte[] resultIndex = kvdb.getTable().get(consumeQueueKey);
		Assert.assertEquals(index, ByteBuffer.wrap(resultIndex).getLong());
		
		index = 5678;
		queue.nextIndex(index);
        command = consumer.buildAckOut(0, queue);
        command.position(COMMAND_DATA_OFFSET);
        recordData = ByteBuffer.allocate(REOCRD_BODY_OFFSET + command.remaining());
        recordData.putInt(REOCRD_HEAD_LENGTH + command.remaining()); // record length
        recordData.putShort((short) 0);                              // record type
        recordData.putLong(MQUtil.currentTimeMillis());              // record time stamp
        recordData.put(command);
        recordData.flip();
		
        kvdb.add(recordData.duplicate());
        
        resultIndex = kvdb.getTable().get(consumeQueueKey);
        Assert.assertEquals(index, ByteBuffer.wrap(resultIndex).getLong());
	}
	
	@Test
	public void wrapAndUnwrapTest() throws Exception {
		MQKVdb kvdb = new MQKVdb();
		MQConsumer consumer = new MQConsumer(null, new MQClusterMetaData(null));
		
		long index_1 = 100;
		MQQueue queue_1 = createMQQueue("testTopic_1", MQGroup.GROUP_1, index_1);
		ByteBuffer command_1 = consumer.buildAckOut(0, queue_1);
		command_1.position(COMMAND_DATA_OFFSET);
		ByteBuffer recordData_1 = ByteBuffer.allocate(REOCRD_BODY_OFFSET + command_1.remaining());
		recordData_1.putInt(REOCRD_HEAD_LENGTH + command_1.remaining()); // record length
		recordData_1.putShort((short) 0);                                // record type
		recordData_1.putLong(MQUtil.currentTimeMillis());                // record time stamp
		recordData_1.put(command_1);
		recordData_1.flip();
		kvdb.add(recordData_1);
		
		long index_2 = 200;
		MQQueue queue_2 = createMQQueue("testTopic_2", MQGroup.GROUP_1, index_2);
		ByteBuffer command_2 = consumer.buildAckOut(0, queue_2);
		command_2.position(COMMAND_DATA_OFFSET);
		ByteBuffer recordData_2 = ByteBuffer.allocate(REOCRD_BODY_OFFSET + command_2.remaining());
		recordData_2.putInt(REOCRD_HEAD_LENGTH + command_2.remaining()); // record length
		recordData_2.putShort((short) 0);                                // record type
		recordData_2.putLong(MQUtil.currentTimeMillis());                // record time stamp
		recordData_2.put(command_2);
		recordData_2.flip();
		kvdb.add(recordData_2);
		
		ByteBuffer checkPointWrapper = kvdb.wrapCheckPoint();
		kvdb = MQKVdb.unwrap(checkPointWrapper);
		
		ByteBuffer queryData = consumer.buildUpdateOut(0, queue_1);
		String consumeQueueKey = ByteBufUtil.hexDump(queryData.array(), COMMAND_DATA_OFFSET, queryData.remaining() - COMMAND_DATA_OFFSET);
		byte[] resultIndex = kvdb.getTable().get(consumeQueueKey);
		Assert.assertEquals(index_1, ByteBuffer.wrap(resultIndex).getLong());
		
		queryData = consumer.buildUpdateOut(0, queue_2);
		consumeQueueKey = ByteBufUtil.hexDump(queryData.array(), COMMAND_DATA_OFFSET, queryData.remaining() - COMMAND_DATA_OFFSET);
		resultIndex = kvdb.getTable().get(consumeQueueKey);
		Assert.assertEquals(index_2, ByteBuffer.wrap(resultIndex).getLong());
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
	
	enum MQGroup {
		GROUP_1
	}
	 
}

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

import static org.nopasserby.fastminimq.MQUtil.nextUUID;
import static org.nopasserby.fastminimq.ReocrdHelper.createCommitTransactionReocrd;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import static org.nopasserby.fastminimq.ReocrdHelper.createPrepareTransactionReocrd;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.ConsumeQueueIndex.QueryIndex;

public class ConsumeQueueIndexTest {

	@Test
	public void createIndexTest() throws Exception {
		ConsumeQueueIndex consumeQueueIndex = new ConsumeQueueIndex(100, true);
		String topic = "testTopic";
		int batch = 1000;
		List<Long> recordOffsetList = new ArrayList<Long>();
		List<Integer> recordDataLengthList = new ArrayList<Integer>();
		List<Long> recordTimestampList = new ArrayList<Long>();
		long recordOffset = 0;
		for (int index = 0; index < batch; index++) {
			ByteBuffer recordData = createNonTransactionRecord(topic);
			long recordTimestamp = recordData.getLong(6);
			int recordDataLength = recordData.remaining();
			consumeQueueIndex.createIndex(recordOffset, recordData);
			recordOffsetList.add(recordOffset);
			recordDataLengthList.add(recordDataLength);
			recordTimestampList.add(recordTimestamp);
			recordOffset += recordDataLength;
		}
		
		consumeQueueIndex.clearInvalidCache(recordOffset + 1);
		
		for (int index = 0; index < batch;) {
			QueryIndex queryIndex = consumeQueueIndex.queryIndex(topic, index);
			while (queryIndex.next()) {
				long currentRecordOffset = queryIndex.currentRecordOffset();
				int currentRecordLength = queryIndex.currentRecordLength();
				long currentRecordTimestamp = queryIndex.currentRecordTimestamp();
				
				long targetRecordOffset = recordOffsetList.get(index);
				Assert.assertEquals(currentRecordOffset, targetRecordOffset);
				
				int targetRecordLength = recordDataLengthList.get(index);
				Assert.assertEquals(currentRecordLength, targetRecordLength);
				
				long targetRecordTimestamp = recordTimestampList.get(index);
				Assert.assertEquals(currentRecordTimestamp, targetRecordTimestamp);
				
				index++;
			}
			queryIndex.release();
		}
		
		consumeQueueIndex.delete();
	}
	
	/**
	* 1.请求 index > 已解析 lastIndex，则下次请求 index 仍等于该 index
	* 2.请求 index < 已解析 lastIndex
	* 2.1 请求 topic 无 startIndex，则下次请求 index 仍等于该 index
	* 2.2 请求 topic 有 startIndex
	* 2.2.1 topic startIndex > index，则下次请求 index 跳到 startIndex
	* 2.2.2 topic startIndex <= index
	* 2.2.2.1 当前 subqueue 属于最后一个 subqueue
	* 2.2.2.1.1 此时不存在此 topic 的 subqueueIndex，则下次请求 index 仍等于该 index
	* 2.2.2.1.2 此时存在此 topic 的 subqueueIndex，但所访问 index 超出范围，则下次请求 index 仍等于该 index
	* 2.2.2.1.3 此时存在此 topic 的 subqueueIndex，访问 index 未超出范围，则返回相应记录
	* 2.2.2.2 当前 subqueue 不属于最后一个 subqueue
	* 2.2.2.2.1 此时不存在此 topic 的 subqueueIndex，则下次请求 index 跳到下一个 subqueue
	* 2.2.2.2.2 此时存在此 topic 的 subqueueIndex，但所访问 index 超出范围，则下次请求 index 跳到下一个 subqueue
	* 2.2.2.2.3 此时存在此 topic 的 subqueueIndex，访问 index 未超出范围，则返回相应记录
	* 
	*/
	@Test
	public void queryIndexTest() throws Exception {
		int batch = 1000;
		int subqueueCapacity = 100;
		ConsumeQueueIndex consumeQueueIndex = new ConsumeQueueIndex(subqueueCapacity, true);
		String[] topicArray = new String[] {"testTopic_1", "testTopic_2"};
		long recordOffset = 0;
		ByteBuffer recordData = null;
		for (int index = 0; index < batch; index++) {
			if (index == 501) {
				recordData = createNonTransactionRecord("testTopic_3");
			} else {				
				recordData = createNonTransactionRecord(topicArray[index % topicArray.length]);
			}
			int recordLength = recordData.remaining();
			consumeQueueIndex.createIndex(recordOffset, recordData);
			recordOffset += recordLength;
		}
		
		
		/** 1.请求 index > 已解析 lastIndex，则下次请求 index 仍等于该 index */
		int index = 1001;
		QueryIndex queryIndex = consumeQueueIndex.queryIndex("testTopic_1", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(index, queryIndex.nextAvailableIndex(index));
		
		/**
		 * 2.请求 index < 已解析 lastIndex
		 * 2.1 请求 topic 无 startIndex，则下次请求 index 仍等于该 index
		 * */
		index = 0;
		queryIndex = consumeQueueIndex.queryIndex("testTopic_xx", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(index, queryIndex.nextAvailableIndex(index));
		
		/**
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.1 topic startIndex > index，则下次请求 index 跳到 startIndex
		 * */
		index = 0;
		int nextIndex = 500;
		queryIndex = consumeQueueIndex.queryIndex("testTopic_3", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(nextIndex, queryIndex.nextAvailableIndex(index));
		
		/** 
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.2 topic startIndex <= index
		 * 2.2.2.1 当前 subqueue 属于最后一个 subqueue
		 * 2.2.2.1.1 此时不存在此 topic 的 subqueueIndex，则下次请求 index 仍等于该 index
		 * */
		index = 910; 
		queryIndex = consumeQueueIndex.queryIndex("testTopic_xx", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(index, queryIndex.nextAvailableIndex(index));
		queryIndex.release();
		
		/** 
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.2 topic startIndex <= index
		 * 2.2.2.1 当前 subqueue 属于最后一个 subqueue
		 * 2.2.2.1.2 此时存在此 topic 的 subqueueIndex，但所访问 index 超出范围，则下次请求 index 仍等于该 index
		 * */
		index = 960;// last sub queue index [900, 949]
		queryIndex = consumeQueueIndex.queryIndex("testTopic_1", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(index, queryIndex.nextAvailableIndex(index));
		queryIndex.release();
		
		/** 
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.2 topic startIndex <= index
		 * 2.2.2.1 当前 subqueue 属于最后一个 subqueue
		 * 2.2.2.1.3 此时存在此 topic 的 subqueueIndex，访问 index 未超出范围，则返回相应记录
		 * */
		index = 940;// last sub queue index [900, 949]
		int rows = 1;
		queryIndex = consumeQueueIndex.queryIndex("testTopic_1", index);
		Assert.assertEquals(true, queryIndex.hasAvailableIndex());
		Assert.assertEquals(index + rows, queryIndex.nextAvailableIndex(index + rows));
		queryIndex.release();
		
		/** 
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.2 topic startIndex <= index
		 * 2.2.2.2 当前 subqueue 不属于最后一个 subqueue
		 * 2.2.2.2.1 此时不存在此 topic 的 subqueueIndex，则下次请求 index 跳到下一个 subqueue
		 * */
		index = 810; 
		nextIndex = 900;
		queryIndex = consumeQueueIndex.queryIndex("testTopic_3", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(nextIndex, queryIndex.nextAvailableIndex(index));
		queryIndex.release();
		
		/** 
		 * 2.请求 index < 已解析 lastIndex
		 * 2.2 请求 topic 有 startIndex
		 * 2.2.2 topic startIndex <= index
		 * 2.2.2.2 当前 subqueue 不属于最后一个 subqueue
		 * 2.2.2.2.2 此时存在此 topic 的 subqueueIndex，但所访问 index 超出范围，则下次请求 index 跳到下一个 subqueue
		 * */
		index = 860;// [800, 849]
		nextIndex = 900;
		queryIndex = consumeQueueIndex.queryIndex("testTopic_1", index);
		Assert.assertEquals(false, queryIndex.hasAvailableIndex());
		Assert.assertEquals(nextIndex, queryIndex.nextAvailableIndex(index));
		queryIndex.release();
		
		consumeQueueIndex.delete();
	}
	
	@Test
	public void filterTest() throws Exception {
		int batch = 1000;
		int subqueueCapacity = 100;
		ConsumeQueueIndex consumeQueueIndex = new ConsumeQueueIndex(subqueueCapacity, true);
		String topic = "testTopic";
		List<Long> recordOffsetList = new ArrayList<Long>();
		List<Integer> recordLengthList = new ArrayList<Integer>();
		long recordOffset = 0;
		for (int index = 0; index < batch; index++) {
			byte[] txid = nextUUID();
			ByteBuffer recordData_1 = createPrepareTransactionReocrd(txid, topic);
			int recordLength_1 = recordData_1.remaining();
			consumeQueueIndex.createIndex(recordOffset, recordData_1);
			recordOffset += recordLength_1;
			
			// second valid
			ByteBuffer recordData_2 = createCommitTransactionReocrd(txid, topic);
			int recordLength_2 = recordData_2.remaining();
			consumeQueueIndex.createIndex(recordOffset, recordData_2);
			recordOffsetList.add(recordOffset);
			recordLengthList.add(recordLength_2);
			recordOffset += recordLength_2;
			// third invalid
			ByteBuffer recordData_3 = createCommitTransactionReocrd(txid, topic);
			consumeQueueIndex.createIndex(recordOffset, recordData_3);
		}
		
		consumeQueueIndex.clearInvalidCache(recordOffset + 1);
		
		for (int index = 0; index < batch;) {
			QueryIndex queryIndex = consumeQueueIndex.queryIndex(topic, index);
			while (queryIndex.next()) {
				long currentRecordOffset = queryIndex.currentRecordOffset();
				int currentRecordLength = queryIndex.currentRecordLength();
				
				long targetRecordOffset = recordOffsetList.get(index);
				Assert.assertEquals(currentRecordOffset, targetRecordOffset);
				
				int targetRecordLength = recordLengthList.get(index);
				Assert.assertEquals(currentRecordLength, targetRecordLength);
				index++;
			}
			queryIndex.release();
		}
		
		consumeQueueIndex.delete();
	}
	
}

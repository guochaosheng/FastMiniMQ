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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_BATCH_READ_LENGTH;
import org.junit.Assert;
import org.junit.Test;

public class ConsumeQueueTopicIdIndexTest {

	@Test
	public void autoCreateTopicIdTest() throws Exception {
		ConsumeQueueTopicIdIndex consumeQueueTopicIdIndex = new ConsumeQueueTopicIdIndex(false);
		int topicId_1 = consumeQueueTopicIdIndex.createTopicId("testTopic_1", 0);
		Assert.assertEquals(0, topicId_1);
		
		int topicId_2_1 = consumeQueueTopicIdIndex.createTopicId("testTopic_2", 0);
		int topicId_2_2 = consumeQueueTopicIdIndex.createTopicId("testTopic_2", 0);
		Assert.assertEquals(1, topicId_2_1);
		Assert.assertEquals(1, topicId_2_2);
		
		int topicId_3 = consumeQueueTopicIdIndex.createTopicId("testTopic_3", 0);
		Assert.assertEquals(2, topicId_3);
		
		Integer topicId_xx = consumeQueueTopicIdIndex.getTopicId("testTopic_xx");
		Assert.assertNull(topicId_xx);
		
		consumeQueueTopicIdIndex.delete();
	}
	
	@Test
	public void reloadTest() throws Exception {
		ConsumeQueueTopicIdIndex consumeQueueTopicIdIndex = new ConsumeQueueTopicIdIndex(false);
		int index = 0;
		while (consumeQueueTopicIdIndex.length() < MAX_BATCH_READ_LENGTH << 1) {
			String topic = "testTopic_" + (index++);
			consumeQueueTopicIdIndex.createTopicId(topic, 0);
		}
		consumeQueueTopicIdIndex.close();
		consumeQueueTopicIdIndex = new ConsumeQueueTopicIdIndex();
		
		while (index > 0) {
			String topic = "testTopic_" + (--index);
			int topicId_1 = consumeQueueTopicIdIndex.getTopicId(topic);
			Assert.assertEquals(index, topicId_1);
		}
		
		consumeQueueTopicIdIndex.delete();
	}
	
}

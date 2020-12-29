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
import static org.nopasserby.fastminimq.MQConstants.LogType.consume_topicid_index;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_BATCH_READ_LENGTH;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;

public class ConsumeQueueTopicIdIndex {
    
    private Map<String, TopicIndex> topicIdMap = new ConcurrentHashMap<String, TopicIndex>();
    
    private MQNodeLog topicIdIndex;
    
    private int topicIdCount;
    
    private boolean flushSync;
    
    public ConsumeQueueTopicIdIndex(boolean flushSync) throws Exception {
        this.flushSync = flushSync;
        this.topicIdIndex = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + consume_topicid_index));
        this.init();
    }
    
    public ConsumeQueueTopicIdIndex() throws Exception {
        this(true);
    }
    
    private void init() throws Exception {
        long length = this.topicIdIndex.writeOffset();
        long segmentLength = MAX_BATCH_READ_LENGTH;
        int position = 0;
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, segmentLength));
        while (length > 0) {
            long nextLength = Math.min(length, segmentLength);
            topicIdIndex.read(buffer, position);
            buffer.flip();
            while (buffer.remaining() > Short.BYTES) {
                short topicLength = buffer.getShort();
                byte[] dstTopic = new byte[topicLength];
                if (buffer.remaining() < topicLength + Long.BYTES) {
                    break;
                }
                buffer.get(dstTopic);
                String topic = new String(dstTopic);
                long startIndex = buffer.getLong();
                topicIdMap.put(topic, new TopicIndex(topicIdCount, startIndex));
                position += Short.BYTES + topicLength + Long.BYTES;
                topicIdCount++;
            }
            length -= nextLength;
            buffer.clear();
        }
    }
    
    public long length() {
        return topicIdIndex.writeOffset();
    }
    
    TopicIndex autoCreateTopicIndex(String topic, long index) throws Exception {
        TopicIndex topicIndex = topicIdMap.get(topic);
        if (topicIndex == null) {
            topicIndex = appendTopic(topic, index);
            topicIdMap.put(topic, topicIndex);
        }
        return topicIndex;
    }
    
    public Integer createTopicId(String topic, long startIndex) throws Exception {
        return autoCreateTopicIndex(topic, startIndex).topicId;
    }
    
    Integer getTopicId(String topic) throws Exception {
        TopicIndex topicIndex = getTopicIndex(topic);
        return topicIndex == null ? null : topicIndex.getTopicId();
    }
    
    TopicIndex getTopicIndex(String topic) throws Exception {
        return topicIdMap.get(topic);
    }

    TopicIndex appendTopic(String topic, long startIndex) throws Exception {
        byte[] topicbuf = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(topicbuf.length + Short.BYTES + Long.BYTES);
        buffer.putShort((short) topicbuf.length);
        buffer.put(topicbuf);
        buffer.putLong(startIndex);
        buffer.flip();
        topicIdIndex.write(buffer);
        topicIdIndex.flush();
        if (flushSync) {            
            topicIdIndex.sync();
        }
        return new TopicIndex(topicIdCount++, startIndex);
    }

    public void delete() throws Exception {
        topicIdMap.clear();
        topicIdIndex.delete();
    }
    
    public void close() throws Exception {
        topicIdMap.clear();
        topicIdIndex.close();
    }
    
    class TopicIndex {
        Integer topicId;
        long startIndex;
        
        public TopicIndex(Integer topicId, long startIndex) {
            this.topicId = topicId;
            this.startIndex = startIndex;
        }
        
        public Integer getTopicId() {
            return topicId;
        }
        
        public long getStartIndex() {
            return startIndex;
        }
    }

}

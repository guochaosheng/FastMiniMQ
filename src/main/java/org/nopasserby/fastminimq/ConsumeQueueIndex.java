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

import static org.nopasserby.fastminimq.MQConstants.COMMIT_TX;
import static org.nopasserby.fastminimq.MQConstants.GLOBAL_ID_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MAGIC;
import static org.nopasserby.fastminimq.MQConstants.NON_TX;
import static org.nopasserby.fastminimq.MQConstants.PRE_TX;
import static org.nopasserby.fastminimq.MQConstants.ROLLBACK_TX;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.INDEX_SUBQUEUE_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_LENGTH_SELF_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.startThread;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

import org.nopasserby.fastminimq.ConsumeQueueTopicIdIndex.TopicIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

public class ConsumeQueueIndex implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ConsumeQueueIndex.class);
    
    private static final int RECORD_INDEX_UNIT_LENGTH = 20;
    
    private int subqueueCapacity;

    private GlobalIdIndex globalIdIndex;
    
    private MQKVdb kvdb;
    
    private ConsumeQueueTopicIdIndex topicIdIndex;
    
    private SubqueueIndex subqueueIndex;
    
    private NavigableMap<Long, SubqueueIndex> subqueueMapcache = new ConcurrentSkipListMap<Long, SubqueueIndex>();
    
    private ConsumeQueueCheckPoint consumeQueueCheckPoint;
    
    private volatile long lastIndex = -1;
    
    public ConsumeQueueIndex() throws Exception {
        this(INDEX_SUBQUEUE_LENGTH);
    }
    
    public ConsumeQueueIndex(int subqueueCapacity) throws Exception {
        this(subqueueCapacity, false);
    }
    
    public ConsumeQueueIndex(int subqueueCapacity, boolean checkpointImmediately) throws Exception {
        this.topicIdIndex = new ConsumeQueueTopicIdIndex();
        this.consumeQueueCheckPoint = new ConsumeQueueCheckPoint(checkpointImmediately);
        this.kvdb = new MQKVdb();
        this.globalIdIndex = new GlobalIdIndex();
        this.subqueueCapacity = subqueueCapacity;
    }
    
    public MQKVdb getKVdb() {
        return kvdb;
    }
    
    public void createIndex(long startOffset, ByteBuffer recordData) throws Exception {
        long recordOffset = startOffset;
        int recordDataLength = recordData.remaining();
        
        /**
         * | record length | record type | record time stamp  |    record body     |
         * |---------------|-------------|--------------------|--------------------|
         * |   XXXXXXXX    |    XXXX     | XXXXXXXX XXXXXXXX  |          N         |
         * 
         * */
        int recordLength = recordData.getInt();
        
        if (recordDataLength != (REOCRD_LENGTH_SELF_LENGTH + recordLength)) {
            throw new IllegalAccessException();
        }
        
        if (kvdb.isDynamic(recordData)) {
            kvdb.add(recordData);
            return;
        }
        
        short recordType = recordData.getShort();
        long recordTimestamp = recordData.getLong();
        
        /**
         * |----------------------------------------------------------------------------------------------------------------------------------------------|
         * | magic | sign |              global id              | topic length | topic | body length | body |   body  crc    | producer length | producer |
         * |-------|------|-------------------------------------|--------------|-------|-------------|------|----------------|-----------------|----------|
         * |   XX  |  XX  | XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX |     XXXX     |   N   |     XXXX    |   N  |    XXXX XXXX   |      XXXX       |     N    |
         * 
         * */
        byte magic = recordData.get();
        if (magic != MAGIC) {
            throw new IllegalAccessException("offset " + startOffset + " record magic[" + magic + "] not equal " + MAGIC);
        }
        
        byte sign = recordData.get();
        ByteBuffer recordBody = recordData;
        if (isFilterOut(sign, recordOffset, recordDataLength, recordType, recordTimestamp, recordBody)) {
            return;
        }
        
        short topicLength = recordData.getShort();
        byte[] topicbuf = new byte[topicLength];
        recordData.get(topicbuf);
        String topic = new String(topicbuf);
        
        long recordIndex = lastIndex + 1;
        
        Integer topicId = topicIdIndex.getTopicId(topic);
        if (topicId == null) {
            topicId = topicIdIndex.createTopicId(topic, recordIndex / subqueueCapacity * subqueueCapacity);
        }
        
        createIndex(topicId, recordIndex, recordOffset, recordDataLength, recordTimestamp);
        
        lastIndex = recordIndex;
    }
    
    public void delete() throws Exception {
        if (subqueueIndex != null) {
            subqueueIndex.release();
            subqueueIndex = null;
        }
        subqueueMapcache.clear();
        kvdb.clear();
        globalIdIndex.clear();
        
        topicIdIndex.delete();
        consumeQueueCheckPoint.delete();
    }
    
    @Override
    public void run() {
        startThread(consumeQueueCheckPoint, "MQ-BROKER-CONSUMEQUEUE-CHECKPOINT");
    }
    
    public void shutdown() throws Exception {
        consumeQueueCheckPoint.shutdown();
    }
    
    boolean isFilterOut(byte sign, long recordOffset, int recordDataLength, short recordType, long recordTimestamp, ByteBuffer recordBody) {
        if (sign == NON_TX) {
            recordBody.position(recordBody.position() + GLOBAL_ID_LENGTH);
            return false;
        }
        
        byte[] dstId = new byte[GLOBAL_ID_LENGTH]; 
        recordBody.get(dstId);
        String globalId = ByteBufUtil.hexDump(dstId);
        
        if (sign == PRE_TX) {
            globalIdIndex.add(globalId, recordOffset + recordDataLength); // only the last message is valid
            return true;
        }
        
        if (sign == COMMIT_TX || sign == ROLLBACK_TX) {
            if (!globalIdIndex.remove(globalId)) {
                // skip duplicate and no contain id
                return true;
            }
        }
        
        return false;
    }
    
    final static class GlobalIdIndex {
        
        private Map<String, Long> globalIds = new ConcurrentHashMap<String, Long>();
        
        public static GlobalIdIndex unwrap(ByteBuffer checkPointWrapper) {
            GlobalIdIndex globalIdIndex = new GlobalIdIndex();
            while (checkPointWrapper.hasRemaining()) {
                byte[] globalId = new byte[GLOBAL_ID_LENGTH];
                checkPointWrapper.get(globalId);
                Long offset = checkPointWrapper.getLong();
                globalIdIndex.globalIds.put(ByteBufUtil.hexDump(globalId), offset);
            }
            return globalIdIndex;
        }
        
        public void clear() {
            globalIds.clear();
        }

        public ByteBuffer wrapCheckPoint() {
            ByteBuf checkPointWrapper = Unpooled.buffer();
            globalIds.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String globalId, Long offset) {
                    checkPointWrapper.writeBytes(ByteBufUtil.decodeHexDump(globalId));
                    checkPointWrapper.writeLong(offset);
                }
            });
            return checkPointWrapper.nioBuffer();
        }

        public boolean add(String globalId, Long offset) {
            return globalIds.putIfAbsent(globalId, offset) == null;
        }
        
        public boolean remove(String globalId) {
            return globalIds.remove(globalId) != null;
        }

        public boolean hasIndex() {
            return !globalIds.isEmpty();
        }
        
        public void deleteExpiredIndex(long offset) {
            List<String> invalidGlobalIds = new ArrayList<String>();
            globalIds.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String globalId, Long globalIdOffset) {
                    if (globalIdOffset < offset) {
                        invalidGlobalIds.add(globalId);
                    }
                }
            });
            
            for (String globalId : invalidGlobalIds) {
                globalIds.remove(globalId);
            }
        }
        
    }
    
    public void createIndex(Integer topicId, long recordIndex, long recordOffset, int recordLength, long recordTimestamp) throws Exception {
        if (subqueueIndex == null) {
            subqueueIndex = createSubqueue(recordIndex / subqueueCapacity); 
        }
        
        SubqueueTopicIndex subqueueTopicIndex = subqueueIndex.getTopicIndex(topicId);
        if (subqueueTopicIndex == null) {
            subqueueTopicIndex = subqueueIndex.createTopicIndex(topicId);
        }
        subqueueTopicIndex.writeLong(recordOffset);
        subqueueTopicIndex.writeInt(recordLength);
        subqueueTopicIndex.writeLong(recordTimestamp);
        subqueueTopicIndex.currentIndex++;
        
        if (needCheckPoint(recordIndex)) {
            long checkPointOffset = recordOffset + recordLength;
            createCheckPoint(recordIndex, checkPointOffset);
        }
    }
    
    public SubqueueIndex createSubqueue(long subqueueOrder) {
        SubqueueIndex subqueue = new SubqueueIndex(subqueueOrder); 
        subqueueMapcache.put(subqueueOrder, subqueue);
        return subqueue;
    }
    
    public boolean needCheckPoint(long index) {
        return (index + 1) % subqueueCapacity == 0;
    }

    public void createCheckPoint(long index, long offset) throws Exception {
        subqueueIndex.lastOffset = offset;
        subqueueIndex.finish();
        consumeQueueCheckPoint.createPoint(subqueueIndex, globalIdIndex, kvdb, offset, index);
        subqueueIndex = createSubqueue(index / subqueueCapacity + 1);// next subqueue
    }

    SubqueueTopicIndex reloadIndex(long subqueueOrder, Integer topicId) throws Exception {
        return consumeQueueCheckPoint.reloadIndex(subqueueOrder, topicId);
    }
    
    public void clearInvalidCache(long startAvailableOffset) {
        if (subqueueMapcache.isEmpty()) {
            return;
        }
        
        Long lastSubqueueOrder = subqueueMapcache.lastKey();
        while (true) {
            Entry<Long, SubqueueIndex> entry = subqueueMapcache.firstEntry();
            long subqueueOrder = entry.getKey();
            if (subqueueOrder >= lastSubqueueOrder) {
                break;
            }
            SubqueueIndex subqueue = entry.getValue();
            if (subqueue.lastOffset >= startAvailableOffset || !subqueue.persist) {
                return;
            }
            subqueueMapcache.remove(subqueueOrder);
            subqueue.release();
            
            logger.info("subqueue[order:{}] release, remaining subqueue count {}", subqueueOrder, subqueueMapcache.size());
        }
    }
    
    public void deleteExpiredIndex(long startAvailableOffset) throws Exception {
        globalIdIndex.deleteExpiredIndex(startAvailableOffset);
        consumeQueueCheckPoint.deleteExpiredCheckPoint(startAvailableOffset);
    }
    
    public long resumeCheckPoint(long resumeOffset) throws Exception {
        consumeQueueCheckPoint.resumeCheckPoint(resumeOffset);
        globalIdIndex = consumeQueueCheckPoint.checkPointGlobalIdIndex();
        kvdb = consumeQueueCheckPoint.checkPointKVdb();
        lastIndex = consumeQueueCheckPoint.checkPointIndex();
        return consumeQueueCheckPoint.checkPointOffset();
    }

    public final class QueryIndex {

        ByteBuffer bytebuf;
        
        int nextOffset;
        
        int startOffest;
        
        int endOffset;
        
        long subqueueStartIndex;
        
        long subqueueEndIndex;
        
        boolean isLast;
        
        long nextAvailableIndex;
        
        long recordOffset;
        
        int recordLength;
        
        long recordTimestamp;
        
        public boolean hasAvailableIndex() {
            return bytebuf != null && nextOffset + RECORD_INDEX_UNIT_LENGTH <= endOffset;
        }

        public long nextAvailableIndex(long nextIndex) {
            if (bytebuf == null) {
                return nextAvailableIndex;
            }
            if (bytebuf != null && (nextIndex - 1) == subqueueEndIndex && !isLast) {
                return (nextIndex / subqueueCapacity + 1) * subqueueCapacity;// skip next sub queue start offset
            }
            return nextIndex;
        }

        public void release() {
            bytebuf = null;
            nextOffset = 0;
            subqueueStartIndex = 0;
            subqueueEndIndex = 0;
            isLast = false;
            nextAvailableIndex = 0;
            recordOffset = 0;
            recordLength = 0;
        }
        
        public boolean next() {
            if (hasAvailableIndex()) {
                recordOffset = bytebuf.getLong(nextOffset);
                recordLength = bytebuf.getInt(nextOffset + 8);
                recordTimestamp = bytebuf.getLong(nextOffset + 12);
                nextOffset += RECORD_INDEX_UNIT_LENGTH;
                return true;
            }
            return false;
        }

        public long currentRecordOffset() {
            return recordOffset;
        }

        public int currentRecordLength() {
            return recordLength;
        }

        public long currentRecordTimestamp() {
            return recordTimestamp;
        }
        
        public long startRecordOffset() {
            return bytebuf.getLong(startOffest);
        }
        
        public int startRecordLength() {
            return bytebuf.getInt(startOffest + 8);
        }
        
        public long endRecordOffset() {
            return bytebuf.getLong(endOffset - RECORD_INDEX_UNIT_LENGTH);
        }
        
        public int endRecordLength() {
            return bytebuf.getInt(endOffset - RECORD_INDEX_UNIT_LENGTH + 8);
        }
        
    }
    
    final static class SubqueueIndex {
        volatile long lastOffset;
        long order;
        Map<Integer, SubqueueTopicIndex> topicIndexMap = new ConcurrentHashMap<Integer, SubqueueTopicIndex>();
        boolean persist;
        
        SubqueueIndex(long order) {
            this.order = order;
        }
        
        public void forEach(BiConsumer<Integer, SubqueueTopicIndex> biConsumer) {
            topicIndexMap.forEach(biConsumer);
        }
        
        public void release() {
            forEach(new BiConsumer<Integer, SubqueueTopicIndex>() {
                @Override
                public void accept(Integer topic, SubqueueTopicIndex topicIndex) {
                    topicIndex.release();
                }
            });
        }
        
        public SubqueueTopicIndex getTopicIndex(Integer topicId) {
            return topicIndexMap.get(topicId);
        }
        
        public SubqueueTopicIndex createTopicIndex(Integer topicId) {
            SubqueueTopicIndex topicIndex = new SubqueueTopicIndex();
            topicIndex.order = order;
            topicIndexMap.put(topicId, topicIndex);
            return topicIndex;
        }
        
        public void finish() {
            forEach(new BiConsumer<Integer, SubqueueTopicIndex>() {
                @Override
                public void accept(Integer topic, SubqueueTopicIndex subqueueTopicIndex) {
                    subqueueTopicIndex.startIndex = 0;
                    subqueueTopicIndex.endIndex = subqueueTopicIndex.length() / RECORD_INDEX_UNIT_LENGTH - 1;
                }
            });
        }
        
        public void persist() {
            this.persist = true;
        }
        
    }
    
    FastThreadLocal<QueryIndex> queryIndexLocal = new FastThreadLocal<QueryIndex>() {
        @Override
        protected QueryIndex initialValue() throws Exception {
            return new QueryIndex();
        }
    };
    
    public QueryIndex queryIndex(String topic, long index) throws Exception {
        QueryIndex queryIndex = queryIndexLocal.get();
        queryIndex.nextAvailableIndex = index;
        
        long lastIndex = this.lastIndex;
        if (index > lastIndex) {
            return queryIndex;
        }
        
        TopicIndex topicIndex = topicIdIndex.getTopicIndex(topic);
        if (topicIndex == null) {
            return queryIndex;
        }
        
        if (topicIndex.startIndex > index) {
            queryIndex.nextAvailableIndex = topicIndex.startIndex;
            return queryIndex;
        }
        
        long subqueueOrder = index / subqueueCapacity;
        long lastSubqueueOrder = lastIndex / subqueueCapacity;
        boolean isLast = subqueueOrder == lastSubqueueOrder;
        
        SubqueueTopicIndex subqueueTopicIndex = querySubqueueIndex(subqueueOrder, topicIndex.getTopicId());

        if (subqueueTopicIndex == null && !isLast) {
            queryIndex.nextAvailableIndex = (subqueueOrder + 1) * subqueueCapacity; // skip next sub queue start offset
            return queryIndex;
        }
        
        int relativeIndex = (int) (index % subqueueCapacity);
        
        if (subqueueTopicIndex != null && !isLast && relativeIndex > subqueueTopicIndex.endIndex) {
            queryIndex.nextAvailableIndex = (subqueueOrder + 1) * subqueueCapacity; // skip next sub queue start offset
            return queryIndex;
        }
        
        if (subqueueTopicIndex != null) {
            queryIndex.startOffest = relativeIndex * RECORD_INDEX_UNIT_LENGTH;
            queryIndex.nextOffset = queryIndex.startOffest;
            queryIndex.endOffset = (subqueueTopicIndex.currentIndex + 1) * RECORD_INDEX_UNIT_LENGTH;// reading volatile current index value must be done before get buffer 
            queryIndex.bytebuf = subqueueTopicIndex.buffer();
            queryIndex.isLast = isLast;
            queryIndex.subqueueStartIndex = subqueueTopicIndex.startIndex;
            queryIndex.subqueueEndIndex = subqueueTopicIndex.endIndex;
        }
        
        return queryIndex;
    }
    
    public SubqueueTopicIndex querySubqueueIndex(long subqueueOrder, Integer topicId) throws Exception {
        SubqueueIndex subqueue = subqueueMapcache.get(subqueueOrder);
        if (subqueue == null) {
            return reloadIndex(subqueueOrder, topicId);
        }
        
        SubqueueTopicIndex subqueueTopicIndex = subqueue.getTopicIndex(topicId);
        if (subqueueTopicIndex == null) {
            return reloadIndex(subqueueOrder, topicId);
        }
        
        return subqueueTopicIndex;
    }
    
    final public static class SubqueueTopicIndex {
        volatile int startIndex = -1;
        volatile int endIndex = -1;
        volatile int currentIndex = -1;
        ByteBuf bytebuf;
        long order;
        
        public SubqueueTopicIndex() {
            bytebuf = Unpooled.buffer();
        }
        
        public static SubqueueTopicIndex wrap(long order, byte[] array) {
            SubqueueTopicIndex subqueueTopicIndex = new SubqueueTopicIndex();
            subqueueTopicIndex.bytebuf = Unpooled.wrappedBuffer(array);
            subqueueTopicIndex.startIndex = 0;
            subqueueTopicIndex.endIndex = array.length / RECORD_INDEX_UNIT_LENGTH - 1;
            subqueueTopicIndex.currentIndex = subqueueTopicIndex.endIndex;
            subqueueTopicIndex.order = order;
            return subqueueTopicIndex;
        }
        
        public void release() {
            bytebuf.release();
        }
        
        public void writeInt(int value) {
            bytebuf.writeInt(value);
        }

        public void writeLong(long value) {
            bytebuf.writeLong(value);
        }
        
        public ByteBuffer buffer() {
            int length = bytebuf.readableBytes();
            ByteBuffer buffer = ByteBuffer.wrap(bytebuf.array());
            buffer.limit(length);
            return buffer;
        }
        
        public int length() {
            return bytebuf.readableBytes();
        }
        
    }

}

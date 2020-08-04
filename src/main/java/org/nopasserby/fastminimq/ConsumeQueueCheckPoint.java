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
import static org.nopasserby.fastminimq.MQConstants.MAGIC_V1;
import static org.nopasserby.fastminimq.MQConstants.LogType.consume_subqueue_checkpoint_idx;
import static org.nopasserby.fastminimq.MQConstants.LogType.consume_subqueue_idx;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.SYNC_AFTER_FLUSH;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.nopasserby.fastminimq.ConsumeQueueIndex.GlobalIdIndex;
import org.nopasserby.fastminimq.ConsumeQueueIndex.SubqueueIndex;
import org.nopasserby.fastminimq.ConsumeQueueIndex.SubqueueTopicIndex;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ConsumeQueueCheckPoint implements Runnable {
    
    private static Logger logger = LoggerFactory.getLogger(ConsumeQueueCheckPoint.class);
    
    private static final ByteBuffer EMPTY_POINT = ByteBuffer.allocate(0);
    
    private static final int SUBQUEUE_CHECK_POINT_UNIT_LENGTH = 56;
    
    private BlockingQueue<CheckPoint> checkPointQueue = new ArrayBlockingQueue<CheckPoint>(10);

    private boolean startQueueCheckPoint;
    
    private ByteBuffer checkPointWrapper = ByteBuffer.allocate(SUBQUEUE_CHECK_POINT_UNIT_LENGTH);
    
    private MQNodeLog subqueueIndexLog;
    
    private MQNodeLog subqueueCheckPointIndexLog;
    
    private long checkPointOffset;
    
    private long checkPointIndex = -1;
    
    private long pointOffset;
    
    private boolean checkpointImmediately;
    
    private GlobalIdIndex checkPointGlobalIdIndex;
    
    private MQKVdb checkPointKVdb;
    
    private boolean shutdown;
    
    public ConsumeQueueCheckPoint(boolean checkpointImmediately) throws Exception {
        this(checkpointImmediately, 
                new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + consume_subqueue_idx)), 
                new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + consume_subqueue_checkpoint_idx)));
    }
    
    public ConsumeQueueCheckPoint(boolean checkpointImmediately, MQNodeLog subqueueIndexLog, MQNodeLog subqueueCheckPointIndexLog) throws Exception {
        this.checkpointImmediately = checkpointImmediately;
        this.subqueueIndexLog = subqueueIndexLog;
        this.subqueueCheckPointIndexLog = subqueueCheckPointIndexLog;
    }
    
    public void resumeCheckPoint(long resumeOffset) throws Exception {
        logger.info("check point index log start offset is {}, end offset is {}", 
                subqueueCheckPointIndexLog.startOffset(), subqueueCheckPointIndexLog.writeOffset());
        
        // TODO when exception exit check file point non exist
        long checkPointOffset = 0;
        long checkPointIndex = -1;
        long pointOffset = 0;
        long offset = subqueueCheckPointIndexLog.startOffset();
        long globalIDPointOffset = 0, kvdbPointOffset = 0;
        int globalIDPointLength = 0, kvdbPointLength = 0; // TODO need to support capacity greater than integer maximum?
        ByteBuffer checkPointWrapper = ByteBuffer.allocate(SUBQUEUE_CHECK_POINT_UNIT_LENGTH);
        while (true) {
            if (subqueueCheckPointIndexLog.readFlip(checkPointWrapper, offset) != SUBQUEUE_CHECK_POINT_UNIT_LENGTH 
                    || !checkCheckPointAvailable(checkPointWrapper, resumeOffset)) {
                break;
            }
            checkPointOffset = checkPointWrapper.getLong();
            checkPointIndex = checkPointWrapper.getLong();
            globalIDPointOffset = checkPointWrapper.getLong();
            globalIDPointLength = checkPointWrapper.getInt();
            kvdbPointOffset = checkPointWrapper.getLong();
            kvdbPointLength = checkPointWrapper.getInt();
            pointOffset = checkPointWrapper.getLong() + checkPointWrapper.getInt();// last available point start offset + length
            
            checkPointWrapper.clear();
            
            offset += SUBQUEUE_CHECK_POINT_UNIT_LENGTH;
        }
        
        this.checkPointOffset = checkPointOffset;
        this.checkPointIndex = checkPointIndex;
        this.pointOffset = pointOffset;
        
        subqueueCheckPointIndexLog.truncate(offset);
        subqueueIndexLog.truncate(pointOffset);
        
        ByteBuffer globalIDPoint = ByteBuffer.allocate(globalIDPointLength);
        subqueueIndexLog.readFlip(globalIDPoint, globalIDPointOffset);
        checkPointGlobalIdIndex = GlobalIdIndex.unwrap(globalIDPoint);
        
        ByteBuffer kvdbPoint = ByteBuffer.allocate(kvdbPointLength);
        subqueueIndexLog.readFlip(kvdbPoint, kvdbPointOffset);
        checkPointKVdb = MQKVdb.unwrap(kvdbPoint);
    }
    
    public void deleteExpiredCheckPoint(long startAvailableOffset) throws Exception {
        long offset = 0, startPointOffset = 0;
        ByteBuffer checkPointWrapper = ByteBuffer.allocate(SUBQUEUE_CHECK_POINT_UNIT_LENGTH);
        while (true) {
            if (subqueueCheckPointIndexLog.readFlip(checkPointWrapper, offset) != SUBQUEUE_CHECK_POINT_UNIT_LENGTH 
                    || !checkCheckPointAvailable(checkPointWrapper)) {
                break;
            }
            long checkPointOffset = checkPointWrapper.getLong(0);
            startPointOffset = checkPointWrapper.getLong(16);
            if (checkPointOffset > startAvailableOffset) {
                break;
            }
            
            offset += SUBQUEUE_CHECK_POINT_UNIT_LENGTH;
        }
        
        subqueueIndexLog.deleteSegmentLog(startPointOffset);
        subqueueCheckPointIndexLog.deleteSegmentLog(offset);
    }
    
    public long checkPointOffset() {
        return checkPointOffset;
    }
    
    public long checkPointIndex() {
        return checkPointIndex;
    }
    
    public GlobalIdIndex checkPointGlobalIdIndex() {
        return checkPointGlobalIdIndex;
    }
    
    public MQKVdb checkPointKVdb() {
        return checkPointKVdb;
    }
    
    boolean checkCheckPointAvailable(ByteBuffer checkPointWrapper) {
        return checkPointWrapper.getInt(checkPointWrapper.remaining() - Integer.BYTES) == MAGIC_V1;
    }
    
    boolean checkCheckPointAvailable(ByteBuffer checkPointWrapper, long startOffset) {
        return checkPointWrapper.getInt(checkPointWrapper.remaining() - Integer.BYTES) == MAGIC_V1 && checkPointWrapper.getLong(0) <= startOffset;
    }

    class CheckPoint {
        ByteBuffer globalIDPoint = EMPTY_POINT;
        long globalIDPointOffset;
        int globalIDPointLength;
        
        ByteBuffer kvdbPoint = EMPTY_POINT;
        long kvdbPointOffset;
        int kvdbPointLength;
        
        SubqueueIndex subqueueIndex;
        ByteBuffer point;
        long pointOffset;
        int pointLength;
        
        long checkPointOffset;
        long checkPointIndex;
    }
    
    public void createPoint(SubqueueIndex subqueueIndex, GlobalIdIndex globalIdIndex, MQKVdb kvdbIndex, long offset, long index) throws Exception {
        CheckPoint checkPoint = new CheckPoint();
        
        if (globalIdIndex.hasIndex()) {
            checkPoint.globalIDPoint = globalIdIndex.wrapCheckPoint();
            checkPoint.globalIDPointOffset = pointOffset;
            checkPoint.globalIDPointLength = checkPoint.globalIDPoint.remaining();
            pointOffset += checkPoint.globalIDPointLength;            
        }
        
        if (kvdbIndex.hasIndex()) {            
            checkPoint.kvdbPoint = kvdbIndex.wrapCheckPoint();
            checkPoint.kvdbPointOffset = pointOffset;
            checkPoint.kvdbPointLength = checkPoint.kvdbPoint.remaining();
            pointOffset += checkPoint.kvdbPointLength;
        }
        
        checkPoint.subqueueIndex = subqueueIndex;
        checkPoint.point = buildPoint(subqueueIndex);
        checkPoint.pointOffset = pointOffset;
        checkPoint.pointLength = checkPoint.point.remaining();
        pointOffset += checkPoint.pointLength;
        
        checkPoint.checkPointOffset = offset;
        checkPoint.checkPointIndex = index;
        
        if (checkpointImmediately || !startQueueCheckPoint) {
            buildCheckPoint(checkPoint);
        } else {
            checkPointQueue.put(checkPoint);
            synchronized (checkPointQueue) {                
                checkPointQueue.notify();
            }
        }
    }
    
    private ByteBuffer buildPoint(SubqueueIndex subqueueIndex) {
        ByteBuf pointbuf = Unpooled.buffer();
        subqueueIndex.forEach(new BiConsumer<Integer, SubqueueTopicIndex>() {
            @Override
            public void accept(Integer topic, SubqueueTopicIndex subqueueIndex) {
                int pointLength = subqueueIndex.length();
                pointbuf.writeInt(topic);
                pointbuf.writeLong(pointOffset);
                pointbuf.writeInt(pointLength);
                pointOffset += pointLength;
            }
        });
        return pointbuf.nioBuffer();
    }
    
    @Override
    public void run() {
        startQueueCheckPoint = true;
        while (!isShutdown()) {
            buildCheckPointDoWork();
        }
        logger.info("consume queue checkpoint build has stopped.");
    }
    
    boolean isShutdown() {
        return shutdown;
    }
    
    public void shutdown() throws Exception {
        shutdown = true;
        synchronized (checkPointQueue) {                
            checkPointQueue.notify();
        }
    }
    
    private void buildCheckPointDoWork() {
        try {
            CheckPoint checkPoint = checkPointQueue.poll();
            if (checkPoint != null) {
                buildCheckPoint(checkPoint);
                return;
            }
            synchronized (checkPointQueue) {
                checkPointQueue.wait();
            }
        } catch (Exception e) {
            logger.error("build checkpoint error, waiting queue length is " + checkPointQueue.size(), e);
        }
    }
    
    private void buildCheckPoint(CheckPoint checkPoint) throws Exception {
        ByteBuffer point = checkPoint.point;
        ByteBuffer globalIDPoint = checkPoint.globalIDPoint;
        ByteBuffer kvdbPoint = checkPoint.kvdbPoint;
        SubqueueIndex subqueue = checkPoint.subqueueIndex;
        
        long time0 = System.currentTimeMillis();
        
        if (globalIDPoint.hasRemaining()) {            
            subqueueIndexLog.write(globalIDPoint);
        }
        
        if (kvdbPoint.hasRemaining()) {
            subqueueIndexLog.write(kvdbPoint);
        }
        
        subqueue.forEach(new BiConsumer<Integer, SubqueueTopicIndex>() {
            @Override
            public void accept(Integer topicId, SubqueueTopicIndex subqueueIndex) {
                try {
                    subqueueIndexLog.write(subqueueIndex.buffer());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        subqueueIndexLog.write(point);
        subqueueIndexLog.flush();
        if (SYNC_AFTER_FLUSH) {
            subqueueIndexLog.sync();
        }
        subqueue.persist();
        
        long time1 = System.currentTimeMillis();
        logger.info("storage point use time {} ms", time1 - time0);
        
        subqueueCheckPointIndexLog.write(buildCheckPointIndex(checkPoint));
        subqueueCheckPointIndexLog.flush();
        
        long time2 = System.currentTimeMillis();
        logger.info("storage point index use time {} ms", time2 - time1);
        
        checkPointOffset = checkPoint.checkPointOffset;
        checkPointIndex = checkPoint.checkPointIndex;
        
        if (checkpointImmediately) {
            logger.info("build checkpoint use time {} ms, point offset:{}, point index:{}", time2 - time0, checkPointOffset, checkPointIndex);
        } else {
            logger.info("build checkpoint use time {} ms, point offset:{}, point index:{}, waiting queue length is {}", 
                    time2 - time0, checkPointOffset, checkPointIndex, checkPointQueue.size());
        }
    }
    
    public ByteBuffer buildCheckPointIndex(CheckPoint checkPoint) {
        checkPointWrapper.clear();
        checkPointWrapper.putLong(checkPoint.checkPointOffset);
        checkPointWrapper.putLong(checkPoint.checkPointIndex);
        checkPointWrapper.putLong(checkPoint.globalIDPointOffset);
        checkPointWrapper.putInt(checkPoint.globalIDPointLength);
        checkPointWrapper.putLong(checkPoint.kvdbPointOffset);
        checkPointWrapper.putInt(checkPoint.kvdbPointLength);
        checkPointWrapper.putLong(checkPoint.pointOffset);
        checkPointWrapper.putInt(checkPoint.pointLength);
        checkPointWrapper.putInt(MAGIC_V1);
        checkPointWrapper.flip();
        return checkPointWrapper;
    }
    
    public void delete() throws Exception {
        clear();
        subqueueIndexLog.delete();
        subqueueCheckPointIndexLog.delete();
    }
    
    public void clear() throws Exception {
        checkPointOffset = 0;
        checkPointIndex = -1;
        pointOffset = 0;
        if (checkPointGlobalIdIndex != null) {
            checkPointGlobalIdIndex.clear();
        }
        if (checkPointKVdb != null) {
            checkPointKVdb.clear();
        }
    }
    
    class QueueIndex {
        int capacity = 4;// concurrent consumer group max count, default value is four
        Map<Long, SubqueueTopicIndex> subqueueIndexMapcache = MQUtil.createLRUCache(capacity);
        
        public SubqueueTopicIndex getSubqueueIndex(long subqueueOrder) {
            return subqueueIndexMapcache.get(subqueueOrder);
        }
        
        public void addSubqueueIndex(SubqueueTopicIndex subqueueTopicIndex) {
            subqueueIndexMapcache.putIfAbsent(subqueueTopicIndex.order, subqueueTopicIndex);
        }
    }
    
    private Map<Integer, QueueIndex> queueMapcache = new ConcurrentHashMap<Integer, QueueIndex>();
    
    SubqueueTopicIndex reloadIndex(long subqueueOrder, Integer topicId) throws Exception {
        QueueIndex queueIndex = queueMapcache.get(topicId);
        if (queueIndex == null) {
            return reloadIndex0(subqueueOrder, topicId);
        }
        
        SubqueueTopicIndex subqueueTopicIndex = queueIndex.getSubqueueIndex(subqueueOrder);
        if (subqueueTopicIndex == null) {
            return reloadIndex0(subqueueOrder, topicId);
        }
        
        return subqueueTopicIndex;
    }
    
    private SubqueueTopicIndex reloadIndex0(long subqueueOrder, Integer topicId) throws Exception {
        Point point = reloadPoint(subqueueOrder, topicId);
        if (point == null) {
            return null;
        }
        
        ByteBuffer topicPoint = ByteBuffer.allocate(point.length);
        subqueueIndexLog.readFlip(topicPoint, point.offset);
        
        SubqueueTopicIndex subqueueTopicIndex = SubqueueTopicIndex.wrap(subqueueOrder, topicPoint.array());
        createQueueIndex(topicId).addSubqueueIndex(subqueueTopicIndex);
        
        return subqueueTopicIndex;
    }
    
    private QueueIndex createQueueIndex(Integer topicId) {
        queueMapcache.putIfAbsent(topicId, new QueueIndex());
        return queueMapcache.get(topicId);
    }

    private Map<Long, SubqueuePoint> subqueuePointMapcache = MQUtil.createLRUCache(1024);
    
    private Point reloadPoint(long subqueueOrder, Integer topicId) throws Exception {
        long offset = subqueueOrder * SUBQUEUE_CHECK_POINT_UNIT_LENGTH;
        if (offset >= subqueueCheckPointIndexLog.writeOffset()) {
            return null;
        }
        
        SubqueuePoint subqueuePoint = subqueuePointMapcache.get(subqueueOrder);
        if (subqueuePoint != null) {
            return subqueuePoint.get(topicId);
        }
        subqueuePoint = new SubqueuePoint();
        
        ByteBuffer checkPointWrapper = ByteBuffer.allocate(SUBQUEUE_CHECK_POINT_UNIT_LENGTH);
        subqueueCheckPointIndexLog.readFlip(checkPointWrapper, offset);
        if (!checkPointWrapper.hasRemaining()) {
            return null;
        }
        long pointOffset = checkPointWrapper.getLong(40);
        int pointLength = checkPointWrapper.getInt(48);
        
        ByteBuffer pointbuf = ByteBuffer.allocate(pointLength);
        subqueueIndexLog.readFlip(pointbuf, pointOffset);
        
        while (pointbuf.hasRemaining()) {
            subqueuePoint.put(pointbuf.getInt(), new Point(pointbuf.getLong(), pointbuf.getInt()));
        }
        
        subqueuePointMapcache.putIfAbsent(subqueueOrder, subqueuePoint);
        
        return subqueuePoint.get(topicId);
    }
    
    class SubqueuePoint {
        Map<Integer, Point> subqueueMapcache = new ConcurrentHashMap<Integer, Point>();
        public void put(Integer topicId, Point point) {
            subqueueMapcache.putIfAbsent(topicId, point);
        }
        
        public Point get(Integer topicId) {
            return subqueueMapcache.get(topicId);
        }
    }
    
    class Point {
        long offset;
        int length;
        public Point(long offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }

}

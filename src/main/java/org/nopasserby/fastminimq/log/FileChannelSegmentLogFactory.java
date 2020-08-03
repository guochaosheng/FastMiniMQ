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

package org.nopasserby.fastminimq.log;

import static java.io.File.separator;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_DATA_SEGMENT_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.createDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChannelSegmentLogFactory implements SegmentLogFactory {
    
    private static Logger logger = LoggerFactory.getLogger(FileChannelSegmentLogFactory.class);
    
    private File rootdir;
    
    private long segmentCapacity;
    
    public FileChannelSegmentLogFactory(String rootdir) throws Exception {
        this(rootdir, MAX_DATA_SEGMENT_LENGTH);
    }
    
    public FileChannelSegmentLogFactory(String rootdir, long segmentCapacity) throws Exception {
        this.rootdir = new File(createDir(rootdir));
        this.segmentCapacity = segmentCapacity;
    }
    
    @Override
    public SegmentLog createSegmentLog(long baseOffset) throws Exception {
        String filename = SegmentLog.baseOffset2Filename(baseOffset);
        String pathname = rootdir + separator + filename;
        long startTime = System.currentTimeMillis();
        FileChannelFileSegment segmentLog = new FileChannelFileSegment(new File(pathname), segmentCapacity);
        long endTime = System.currentTimeMillis();
        long useTime = endTime - startTime;
        if (useTime > 1) {
            logger.info("create segment log use time: {} ms", useTime);
        }
        return segmentLog;
    }
    
    public SegmentLog createSegmentLog(File file) throws Exception {
        return new FileChannelFileSegment(file, segmentCapacity);
    }
    
    @Override
    public SegmentLog[] reloadSegmentLog() throws Exception {
        File[] files = rootdir.listFiles();
        SegmentLog[] segmentLogs = new SegmentLog[files.length];
        for (int index = 0; index < segmentLogs.length; index++) {
            segmentLogs[index] = createSegmentLog(files[index]);
        }
        return segmentLogs;
    }
    
    public class FileChannelFileSegment implements SegmentLog {
        File file;
        RandomAccessFile raf;
        FileChannel fileChannel;
        volatile long writeOffset;
        volatile long readOffset;
        long capacity;
        long baseOffset;
        long createTimestamp;
        String filename;
        
        public FileChannelFileSegment(File file, long capacity) throws Exception {
            this.file = file;
            this.capacity = capacity;
            this.filename = file.getName();
            this.baseOffset = SegmentLog.filename2BaseOffset(filename);
            this.createTimestamp = SegmentLog.filename2TimeMillis(filename);
            this.raf = new RandomAccessFile(file, "rw");
            this.fileChannel = raf.getChannel();
            this.writeOffset = raf.length();
        }

        @Override
        public long baseOffset() {
            return baseOffset;
        }

        @Override
        public void flush() throws Exception {
            // do nothing
        }
        
        @Override
        public void sync() throws Exception {
            this.fileChannel.force(false);
        }

        @Override
        public void close() throws Exception {
            if (raf != null) {
                raf.close();
            }
            if (fileChannel != null) {
                fileChannel.close();
            }
        }

        @Override
        public long writeOffset() {
            return writeOffset;
        }

        @Override
        public long readOffset() {
            return readOffset;
        }

        @Override
        public void setReadOffset(long readOffset) throws Exception {
            this.readOffset = readOffset;
        }

        @Override
        public void setWriteOffset(long writeOffset) throws Exception {
            this.writeOffset = writeOffset;
        }

        @Override
        public long remainingCapacity() {
            return capacity - writeOffset;
        }

        @Override
        public long capacity() {
            return capacity;
        }

        @Override
        public int write(ByteBuffer src) throws Exception {
            int length = fileChannel.write(src);
            writeOffset += length;
            return length;
        }

        @Override
        public int write(ByteBuffer src, long offset) throws Exception {
            int length = fileChannel.write(src, offset);
            writeOffset += length;
            return length;
        }

        @Override
        public int write(byte[] src, int srcPos, int length) throws Exception {
            return write(ByteBuffer.wrap(src, srcPos, length));
        }

        @Override
        public int read(ByteBuffer src) throws Exception {
            return fileChannel.read(src);
        }

        @Override
        public int read(ByteBuffer src, long offset) throws Exception {
            return fileChannel.read(src, offset);
        }

        @Override
        public int read(byte[] src, int srcPos, int length) throws Exception {
            return fileChannel.read(ByteBuffer.wrap(src, srcPos, length));
        }

        @Override
        public void truncate(long offset) throws IOException {
            fileChannel.truncate(offset);
            writeOffset = offset;
        }

        @Override
        public void delete() throws Exception {
            this.close();
            this.file.delete();
        }

        @Override
        public long createTimestamp() {
            return createTimestamp;
        }
        
    }
    
}

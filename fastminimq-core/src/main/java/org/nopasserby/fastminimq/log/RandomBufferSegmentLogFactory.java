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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RandomBufferSegmentLogFactory implements SegmentLogFactory {
    
    private File rootdir;
    
    private long segmentCapacity;
    
    private int bufferCapacity;
    
    public RandomBufferSegmentLogFactory(String rootdir) throws Exception {
        this(rootdir, MAX_DATA_SEGMENT_LENGTH, 8192);
    }
    
    public RandomBufferSegmentLogFactory(String rootdir, int bufferCapacity) throws Exception {
        this(rootdir, MAX_DATA_SEGMENT_LENGTH, bufferCapacity);
    }
    
    public RandomBufferSegmentLogFactory(String rootdir, long segmentCapacity, int bufferCapacity) throws Exception {
        this.rootdir = new File(createDir(rootdir));
        this.segmentCapacity = segmentCapacity;
        this.bufferCapacity = bufferCapacity;
    }
    
    @Override
    public SegmentLog createSegmentLog(long baseOffset) throws Exception {
        String filename = SegmentLog.baseOffset2Filename(baseOffset);
        String pathname = rootdir + separator + filename;
        return new RandomBufferFileSegment(new File(pathname), segmentCapacity);
    }
    
    public SegmentLog createSegmentLog(File file) throws Exception {
        return new RandomBufferFileSegment(file, segmentCapacity);
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
    
    class RandomBufferFileSegment implements SegmentLog {
        File file;
        RandomAccessFile raf;
        FileChannel fileChannel;
        byte buf[] = new byte[bufferCapacity];
        int count;
        long writeOffset;
        long readOffset;
        long baseOffset;
        long capacity;
        long createTimestamp;
        String filename;
        
        RandomBufferFileSegment(File file, long capacity) throws Exception {
            this.file = file;
            this.filename = file.getName();
            this.baseOffset = SegmentLog.filename2BaseOffset(filename);
            this.createTimestamp = SegmentLog.filename2TimeMillis(filename);
            this.capacity = capacity;
            this.raf = new RandomAccessFile(file, "rw");
            this.fileChannel = raf.getChannel();
            this.writeOffset = raf.length();
        }
        
        @Override
        public long baseOffset() {
            return baseOffset;
        }
        
        @Override
        public long createTimestamp() {
            return createTimestamp;
        }

        @Override
        public void flush() throws Exception {
            if (count > 0) {
                raf.write(buf, 0, count);
                writeOffset += count;
                count = 0;
            }
        }
        
        @Override
        public void sync() throws Exception {
            this.raf.getFD().sync();
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
            return capacity() - writeOffset;
        }

        @Override
        public long capacity() {
            return capacity;
        }

        @Override
        public int write(ByteBuffer src) throws Exception {
            int pos = src.position();
            int length = write(src.array(), pos, src.remaining());
            src.position(pos + length);
            return length;
        }

        @Override
        public int write(ByteBuffer src, long offset) throws Exception {
            return fileChannel.write(src, offset);
        }

        @Override
        public int write(byte[] src, int srcPos, int len) throws Exception {
            if (len >= buf.length) {
                flush();
                raf.write(src, srcPos, len);
                writeOffset += len;
                return len;
            }
            if (len > buf.length - count) {
                flush();
            }
            System.arraycopy(src, srcPos, buf, count, len);
            count += len;
            return len;
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
        public void truncate(long offset) throws Exception {
            fileChannel.truncate(offset);
            raf.seek(offset);
            writeOffset = offset;
        }

        @Override
        public void delete() throws Exception {
            this.close();
            this.file.delete();
        }

    }
    
}

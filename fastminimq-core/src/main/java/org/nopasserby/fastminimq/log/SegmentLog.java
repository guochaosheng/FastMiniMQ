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

import java.nio.ByteBuffer;

public interface SegmentLog {
    
    long baseOffset();
    
    long createTimestamp();
    
    void flush() throws Exception;
    
    void close() throws Exception;
    
    long writeOffset();
    
    long readOffset();
    
    void setReadOffset(long readOffset) throws Exception;
    
    void setWriteOffset(long writeOffset) throws Exception;
    
    long remainingCapacity();
    
    long capacity();
    
    int write(ByteBuffer src) throws Exception;
    
    int write(ByteBuffer src, long offset) throws Exception;
    
    int write(byte[] src, int srcPos, int length) throws Exception;
    
    int read(ByteBuffer dst) throws Exception;
    
    int read(ByteBuffer dst, long offset) throws Exception;
    
    int read(byte[] dst, int dstPos, int length) throws Exception;
    
    void truncate(long offset) throws Exception;

    void delete() throws Exception;
    
    void sync() throws Exception;
    
    public static long filename2BaseOffset(String filename) {
        return Long.parseLong(filename.substring(21));
    }
    
    public static String baseOffset2Filename(long baseOffset) {
        return String.format("%020d", System.currentTimeMillis()) + String.format("%020d", baseOffset);
    }
    
    public static long filename2TimeMillis(String filename) {
        return Long.parseLong(filename.substring(0, 20));
    }
    
}

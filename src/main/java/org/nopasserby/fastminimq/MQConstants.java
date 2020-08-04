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
import java.util.UUID;

public class MQConstants {
    
    public static final int GLOBAL_ID_LENGTH = 16;
    
    public static final int PARALLEL_CHANNELS = 8;
    
    public static final int RETRY = 3;
    
    public static final int TIMEOUT = 30 * 1000; // default connection timeout 30s
    
    public static final short IMMUTABLE = 0, DYNAMIC = 1;
    
    public static final byte MAGIC = 0x1A;
    
    public static final int MAGIC_V1 = 0xA1B2C3D4;  // magic value < 0
    
    public static final byte NON_TX = 0, PRE_TX = 1, COMMIT_TX = 2, ROLLBACK_TX = 3;

    public static class MQBroker {
        
        public final static String DEFAULT_BROKER_ID = "broker-" + Integer.toHexString(UUID.randomUUID().hashCode());
        
        public final static String BROKER_ID = System.getProperty("broker.id", DEFAULT_BROKER_ID);
        
        public final static String BROKER_HOSTNAME = System.getProperty("hostname", "0.0.0.0:6001");
        
        public final static String DEFAULT_DATA_DIR = System.getProperty("user.dir") + separator + "fastminimq";
        
        public final static String DATA_DIR = System.getProperty("data.dir", DEFAULT_DATA_DIR);
        
        public final static int SERVER_DECODE_MAX_FRAME_LENGTH = 65535;
        
        public final static int CLIENT_DECODE_MAX_FRAME_LENGTH = 16 * 1024 * 1024;
        
        public static final int EVENT_BUFFER_LENGTH = Integer.parseInt(System.getProperty("event.buffer.length", "4096")); // 4096 = 4 * 1024 = 4K 
        
        public static final int EVENT_QUEUE_LENGTH = Integer.parseInt(System.getProperty("event.queue.length", "65536")); // 65536 = 64 * 1024, 4K * (64 * 1024) = 256 M
       
        public static final int BYTES_RINGBUFFER_BLOCKUNIT = Integer.parseInt(System.getProperty("bytes.buffer.blockunit", "67108864")); // 67108864 = 64 * 1024 * 1024 = 64 M
        
        public static final int BYTES_RINGBUFFER_BLOCKSIZE = Integer.parseInt(System.getProperty("bytes.buffer.blocksize", "16")); // 16 * 64 M = 1024 M
        
        public static final int BYTES_BATCH_SIZE = 8 * 1024;
        
        public static final boolean SYNC_AFTER_FLUSH = Boolean.parseBoolean(System.getProperty("flush.sync", "false"));
        
        public final static int KV_DB_SIZE = Integer.parseInt(System.getProperty("kv.db.size", "41943040")); // 40 * 1024 * 1024 = 40 M
        
        public final static int MAX_RECORD_LENGTH = 65535;
        
        public final static int MAX_BATCH_COUNT = 2000;
        
        public static final long MAX_DATA_SEGMENT_LENGTH = Long.parseLong(System.getProperty("data.segment.length", "10737418240")); // 10737418240 = 10 * 1024 * 1024 * 1024 = 10G
        
        public static final long MIN_RESUME_LENGTH = Long.parseLong(System.getProperty("min.resume.length", "4294976296")); // 4294976296 = 4 * 1024 * 1024 * 1024 = 4G
        
        public static final int MAX_BATCH_READ_LENGTH = 512 * 1024;
        
        public static final int INDEX_SUBQUEUE_LENGTH = Integer.parseInt(System.getProperty("data.subqueue.length", "1000000"));
        
        public static final long DATA_RETENTION_MILLIS = Long.parseLong(System.getProperty("data.retention.millis", "60480000")); // 60480000 = 7 * 24 * 60 * 60 * 1000
        
        public static final long DATA_RETENTION_CHECK_INTERVAL_MILLIS = Long.parseLong(System.getProperty("data.retention.check.interval.millis", "3600000")); // 3600000 = 60 * 60 * 1000
        
        public static final String OUT_LOG_LEVEL = System.getProperty("out.log.level", "info");

        public static final String OUT_LOG_FILE = System.getProperty("out.log.file"); // default console output
        
        public static final int OUT_LOG_RETENTION = Integer.parseInt(System.getProperty("out.log.file.retention.days", "30"));
        
    }
    
    public static class MQCommand {
        
        public final static int PRODUCE = 0xA1;
        
        public final static int CONSUME = 0xA2;
        
        public final static int KV_PUT = 0xA3;
        
        public final static int KV_GET = 0xA4;
        
        public final static int KV_DEL = 0xA5;
        
        public static final int ERROR = 0xA9;
        
        /**
         * |------------------------------- command -------------------------------|
         * | command code |     command id    | command data length | command data |
         * |--------------|-------------------|---------------------|--------------|
         * |   XXXX XXXX  | XXXXXXXX XXXXXXXX |       XXXX XXXX     |      N       |
         */
        public static final int COMMAND_CODE_OFFSET = 0;
        
        public static final int COMMAND_CODE_LENGTH = 4;
        
        public static final int COMMAND_ID_OFFSET = COMMAND_CODE_LENGTH;
        
        public static final int COMMAND_ID_OFFSET_LENGTH = 8;
        
        public static final int COMMAND_DATA_LENGTH_OFFSET = COMMAND_ID_OFFSET + COMMAND_ID_OFFSET_LENGTH;
        
        public static final int COMMAND_DATA_LENGTH_SELF_LENGTH = 4;
        
        public static final int COMMAND_DATA_OFFSET = COMMAND_DATA_LENGTH_OFFSET + COMMAND_DATA_LENGTH_SELF_LENGTH;
        
        /**
         * |---- response command data ----|
         * |    status    |     content    |
         * |--------------|----------------|
         * |   XXXX XXXX  |       N        |
         */
        public static final int COMMAND_DATA_RES_STATUS_SELF_LENGTH = 4;
        
        public static final int COMMAND_DATA_RES_CONTENT_OFFSET = COMMAND_DATA_OFFSET + COMMAND_DATA_RES_STATUS_SELF_LENGTH;
        
        /**
         * |-------------------------------- record -------------------------------------|
         * |  record length  |    record type    |   record time stamp   |  record data  |
         * |-----------------|-------------------|-----------------------|---------------|
         * |    XXXX XXXX    |        XXXX       |   XXXXXXXX XXXXXXXX   |       N       |
         * 
         * */
        public static final int REOCRD_LENGTH_SELF_LENGTH = 4;
        
        public static final int REOCRD_TYPE = 2;
        
        public static final int REOCRD_TIMESTAMP_OFFSET = REOCRD_LENGTH_SELF_LENGTH + REOCRD_TYPE;
        
        public static final int REOCRD_TIMESTAMP_LENGTH = 8;
        
        public static final int REOCRD_BODY_OFFSET = REOCRD_TIMESTAMP_OFFSET + REOCRD_TIMESTAMP_LENGTH;
        
        public static final int REOCRD_HEAD_LENGTH = REOCRD_TYPE + REOCRD_TIMESTAMP_LENGTH;
    }

    public enum Status {
        
        OK, FAIL;
        
        public static Status valueOf(int status) {
            for (Status s:Status.values()) {
                if (s.ordinal() == status) {
                    return s;
                }
            }
            return null;
        }
        
    }
    
    public enum LogType {
        
        commit, consume_topicid_index, consume_subqueue_checkpoint_idx, consume_subqueue_idx
        
    }
    
    public static interface ShutdownAble extends Runnable {
        
        public void shutdown() throws Exception;
        
    }
    
}
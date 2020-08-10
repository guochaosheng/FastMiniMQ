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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;
import org.slf4j.Logger;

public class MQUtil {
    
    public static void startThread(Runnable target, String name) {
        Thread thread = new Thread(target);
        thread.setName(name);
        thread.start();
    }
    
    public static void envLog(Logger log, String name) {
        ENV.forEach(new BiConsumer<String, String>() {
            @Override
            public void accept(String key, String value) {
                log.info("{} environment:{}={}", name, key, value);
            }
        });
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void envLoad(String file) throws FileNotFoundException, IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(file));
        System.getProperties().putAll(props);
        ENV.putAll((Map) props);
    }
    
    public static InetSocketAddress toInetAddress(String address) {
        String[] tcpip = address.split(":");
        String ip = tcpip[0];
        int port = Integer.parseInt(tcpip[1]);
        return new InetSocketAddress(ip, port);
    }
    
    public static void checkArgument(boolean expression, String error) {
        if (!expression) {
            throw new IllegalArgumentException(error);
        }
    }
    
    public static int crc(byte[] b) {
        return crc(b, 0, b.length);
    }
    
    public static int crc(byte[] b, int off, int len) {
        CRC32 crc32 = new CRC32();
        crc32.update(b, off, len);
        return (int) crc32.getValue();
    }
    
    public static boolean existsFile(String filepath) {
        return new File(filepath).exists();
    }
    
    public static File createFile(String filepath) throws IOException {
        File file = new File(filepath);
        createDir(file.getParent());
        file.createNewFile();
        return file;
    }
    
    public static String createDir(String dir) throws IOException {
        File file = new File(dir);
        if (file.exists()) {
            if (!file.isDirectory()) {
                throw new IOException(dir + " file exists but it is not a directory.");
            }
            return dir;
        }
        if (!file.mkdirs()) {
            throw new IOException(dir + " mkdirs error.");
        }
        return dir;
    }
    
    public static void deleteFile(String filepath) {
        File file = new File(filepath);
        if (file.isFile()) {
            file.delete();
            return;
        }
        
        File[] files = file.listFiles();
        if (files == null) {
            file.delete();
            return;
        } 
            
        for (int i = 0; i < files.length; i++) {
            deleteFile(files[i].getAbsolutePath());
        }
        file.delete();
    }
    
    public static String throwableToString(Throwable e) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            e.printStackTrace(pw);
        }
        return sw.toString();
    }
    
    public static long currentTimeMillis() {
        return System.currentTimeMillis();
    }
    
    public static byte[] nextUUID() {
        byte[] uuid = new byte[16];
        ByteBuffer buffer = ByteBuffer.wrap(uuid);
        buffer.put(IP);
        buffer.putInt(JVM);
        buffer.putLong(nextId());
        return uuid;
    }
    
    private static final int JVM = (int) (System.currentTimeMillis() >>> 8);
    private static final byte[] IP;
    static {
        byte[] ipadd;
        try {
            ipadd = InetAddress.getLocalHost().getAddress();
        } catch (Exception e) {
            ipadd = ByteBuffer.allocate(4).putInt(new Random().nextInt()).array();
        }
        IP = ipadd;
    }
    
    /**
     * @see https://github.com/twitter/snowflake
     * */
    public static synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        long timestampLeftShift = 22L;
        long twepoch = 1288834974657L;
        long sequenceMask = -1L ^ (-1L << timestampLeftShift);

        if (timestamp < ID_OBJECT.lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds",
                    ID_OBJECT.lastTimestamp - timestamp));
        }

        if (ID_OBJECT.lastTimestamp == timestamp) {
            ID_OBJECT.sequence = (ID_OBJECT.sequence + 1) & sequenceMask;
            if (ID_OBJECT.sequence == 0) {
                while ((timestamp = System.currentTimeMillis()) <= ID_OBJECT.lastTimestamp) {
                }
            }
        } else {
            ID_OBJECT.sequence = 0;
        }

        ID_OBJECT.lastTimestamp = timestamp;
        
        return ((timestamp - twepoch) << timestampLeftShift) | ID_OBJECT.sequence;
    }
    
    private final static IdObject ID_OBJECT = new IdObject();
    
    private static class IdObject {
        long sequence;
        long lastTimestamp;
    }
    
    public static <K, V> Map<K, V> createLRUCache(int capacity) {
        return createLRUCache(capacity, new Object());
    }
    
    public static <K, V> Map<K, V> createLRUCache(int capacity, Object syncObject) {
        return new LRUCache<K, V>(capacity, syncObject);
    }
    
    private static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        
        private static final long serialVersionUID = 5477842595270478311L;
        
        private int capacity;
        
        private Object syncObject;
        
        public LRUCache(int capacity, Object syncObject) {
            super(16, 0.75f, true);
            this.capacity = capacity;
            this.syncObject = syncObject;
        }
        
        @Override
        public V get(Object key) {
            synchronized (syncObject) {
                return super.get(key);
            }
        }
       
        @Override
        public V put(K key, V value) {
            synchronized (syncObject) {
                return super.put(key, value);
            }
        }
        
        @Override
        public V putIfAbsent(K key, V value) {
            synchronized (syncObject) {
                return super.putIfAbsent(key, value);
            }
        }
        
        // TODO ...
        
        @Override
        protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
            return size() > capacity;
        }
        
    }
    
    private final static Map<String, String> ENV = new LinkedHashMap<String, String>();
    
    static {
        ENV.put("fastminimq.version", VersionInfo.version());
        ENV.put("java.version", System.getProperty("java.version", "<NA>"));
        ENV.put("java.vendor", System.getProperty("java.vendor", "<NA>"));
        ENV.put("java.home", System.getProperty("java.home", "<NA>"));
        ENV.put("java.class.path", System.getProperty("java.class.path", "<NA>"));
        ENV.put("java.library.path", System.getProperty("java.library.path", "<NA>"));
        ENV.put("java.io.tmpdir", System.getProperty("java.io.tmpdir", "<NA>"));
        ENV.put("java.compiler", System.getProperty("java.compiler", "<NA>"));
        ENV.put("os.name", System.getProperty("os.name", "<NA>"));
        ENV.put("os.arch", System.getProperty("os.arch", "<NA>"));
        ENV.put("os.version", System.getProperty("os.version", "<NA>"));
        ENV.put("user.name", System.getProperty("user.name", "<NA>"));
        ENV.put("user.home", System.getProperty("user.home", "<NA>"));
        ENV.put("user.dir", System.getProperty("user.dir", "<NA>"));
    }

}

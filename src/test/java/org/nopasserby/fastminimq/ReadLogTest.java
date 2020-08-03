package org.nopasserby.fastminimq;

import java.io.RandomAccessFile;

public class ReadLogTest {
    
    // 11:49:58.251 [main] INFO  o.nopasserby.fastminimq.ReadLogTest - write offset:74415841280, index:9083965
    // 14:24:31.175 [main] INFO  o.n.fastminimq.WriteLogTest2 - write offset:76461096960, index:9333630
    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        RandomAccessFile raf = new RandomAccessFile("C:\\Users\\pell3\\Documents\\eclipse-2020-03-workspace\\FastMiniMQ\\target\\test.data", "rw");
        long length = raf.length();
        System.out.println("length:" + length);
        
        int blockunit = 8 * 1024;
        long offset = length / blockunit * blockunit - blockunit;
        System.out.println("offset:" + offset);
        
//        raf.seek(offset);
//        
//        while (offset < length) {
//            byte[] buffer = new byte[128];
//            raf.read(buffer);
//            offset += buffer.length;
//            
//            System.out.println("offset:" + offset + ", str:" + new String(buffer));
//        }
        
        String str = "testTopic Hello world  Hello world  Hello world  Hello world  Hello world  Hello world  Hello world  Hello world  Hello world  H";
        int code = str.hashCode();
        long count = 0;
        long nextOffset = 0;
        int unit = 1024 * 1024 * 1024;
        long nextPoint = unit;
        while (nextOffset < length) {
            byte[] buffer = new byte[blockunit];
            raf.read(buffer);
            nextOffset += buffer.length;
            
            if (nextOffset > nextPoint) {
                System.out.println("offset:" + nextOffset + ", count:" + count + ", point:" + nextPoint / unit);
                nextPoint += unit;
            }
            
            for (int i = 0; i < 64; i++) {
                String value = new String(buffer, i * 128, 128);
                count++;
                if (code != value.hashCode()) {
                    System.out.println("offset:" + nextOffset + ", count:" + count + ", value:" + value);
                    return;
                }
            }
            
        }
        
    }

}

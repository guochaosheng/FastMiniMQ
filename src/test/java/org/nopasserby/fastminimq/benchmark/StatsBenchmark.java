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

package org.nopasserby.fastminimq.benchmark;

import java.time.LocalTime;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @see https://github.com/apache/rocketmq/blob/master/example/src/main/java/org/apache/rocketmq/example/benchmark/TransactionProducer.java
 * 
 * */
public class StatsBenchmark {
    
    private final int period = Integer.parseInt(System.getProperty("stat.period", "60"));
    
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

    private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.sendRequestSuccessCount.get(),
            this.sendRequestFailedCount.get(),
            this.receiveResponseSuccessCount.get(),
            this.receiveResponseFailedCount.get(),
            this.sendMessageSuccessTimeTotal.get()};

        return snap;
    }
    
    public void start() {
        
        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();
        
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(createSnapshot());
                while (snapshotList.size() > period) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= period) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps =
                        (long)(((end[3] - begin[3]) / (double)(end[0] - begin[0])) * 1000L);
                    final double averageRT = (end[5] - begin[5]) / (double)(end[3] - begin[3]);

                    System.out.printf(
                        "%s Send TPS: %d Max RT: %d Average RT: %7.3f Send Success: %d Response Success: %d Send Failed: %d Response Failed: %d %n",
                        LocalTime.now().withNano(0), sendTps, getSendMessageMaxRT().get(), averageRT, end[1], end[3], end[2], end[4]);
                }
            }

            @Override
            public void run() {
                try {
                    this.printStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, period * 1000, period * 1000);
    }

    public void offerCurrentRT(long currentRT) {
        getSendMessageSuccessTimeTotal().addAndGet(currentRT);
        
        long prevMaxRT = getSendMessageMaxRT().get();
        while (currentRT > prevMaxRT) {
            boolean updated = getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
            if (updated) { 
                break; 
            }
            prevMaxRT = getSendMessageMaxRT().get();
        }
    }
    
    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public AtomicLong getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }

    public AtomicLong getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }

    public AtomicLong getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }
    
}

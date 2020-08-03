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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.nopasserby.fastminimq.MQConstants.Status;

public class MQResult<T> {
    
    private Status status;
    
    private Exception exception;
    
    private T result;
    
    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public static class MQRecord {
        
        private Status status;
        
        private byte[] id;
        
        private byte sign;
        
        private String broker;
        
        private String topic;
        
        private byte[] body;
        
        private long timestamp;
        
        private Exception exception;

        public MQRecord() {
        }
        
        public byte getSign() {
            return sign;
        }

        public void setSign(byte sign) {
            this.sign = sign;
        }

        public byte[] getId() {
            return id;
        }

        public void setId(byte[] id) {
            this.id = id;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public String getBroker() {
            return broker;
        }

        public void setBroker(String broker) {
            this.broker = broker;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public byte[] getBody() {
            return body;
        }

        public void setBody(byte[] body) {
            this.body = body;
        }
        
        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Exception getException() {
            return exception;
        }

        public void setException(Exception exception) {
            this.exception = exception;
        }

    }
    
    public static class MQFuture<T> {
        
        private T result;
        
        private Consumer<T> consumer;
        
        private boolean completed;
        
        public MQFuture() {
        }
        
        public void thenAccept(final Consumer<T> consumer) {
            synchronized (this) {
                this.consumer = consumer;
                if (this.result != null) {
                    this.consumer.accept(this.result);
                }
            }
        }

        public void complete(T result) {
            synchronized (this) {
                if (!completed) {
                    this.result = result;
                    if (consumer != null) {
                        this.consumer.accept(this.result);
                    }
                    this.release();
                }
            }
        }
        
        private void latch(long timeout) throws InterruptedException {
            synchronized (this) {
                while (!completed) {
                    wait(timeout);
                }
            }
        }
        
        private void release() {
            synchronized (this) {
                notify();
                completed = true;
            }
        }

        public T get() throws InterruptedException {
            latch(0);
            return result;
        }
        
        public T get(long timeout, TimeUnit timeUnit) throws InterruptedException {
            latch(timeUnit.toMillis(timeout));
            return result;
        }
        
    }

}

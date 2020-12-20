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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.BROKER_HOST;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BROKER_ID;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.OUT_LOG_FILE;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.OUT_LOG_LEVEL;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.OUT_LOG_RETENTION;

import org.nopasserby.fastminimq.MQBroker.MQBrokerCfg;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

public class FastMiniMQBroker {
    
    /**
     * usage: java -jar FastMiniMQBroker.jar 
     *        or 
     *        java -jar FastMiniMQBroker.jar -c broker.conf
     * 
     * */
    public static void main(String[] args) throws Exception {
        if (args.length > 1 && "-c".equals(args[0])) {
            MQUtil.envLoad(args[1]);
        }
        
        initOut();
        
        bannerOut();
        
        MQBroker broker = new MQBroker(new MQBrokerCfg(BROKER_ID, BROKER_HOST));
        broker.run();
        
        addShutdownHook(broker);
    }
    
    public static void bannerOut() {
        out("  _______  _______  _______  _______  __   __  ___   __    _  ___   __   __  _______  ");
        out(" |       ||   _   ||       ||       ||  |_|  ||   | |  |  | ||   | |  |_|  ||       | ");
        out(" |    ___||  |_|  ||  _____||_     _||       ||   | |   |_| ||   | |       ||   _   | ");
        out(" |   |___ |       || |_____   |   |  |       ||   | |       ||   | |       ||  | |  | ");
        out(" |    ___||       ||_____  |  |   |  |       ||   | |  _    ||   | |       ||  |_|  | ");
        out(" |   |    |   _   | _____| |  |   |  | ||_|| ||   | | | |   ||   | | ||_|| ||      |  ");
        out(" |___|    |__| |__||_______|  |___|  |_|   |_||___| |_|  |__||___| |_|   |_||____||_| ");
        out("                                                                                      ");
    }
    
    public static void out(String s) {
        System.out.printf("%s%n", s);
    }
    
    public static void initOut() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.detachAndStopAllAppenders();

        if (noLogFile()) {
            ConsoleAppender<ILoggingEvent> outputStream = new ConsoleAppender<ILoggingEvent>();
            outputStream.setContext(loggerContext);
            
            initOut(outputStream);
            return;
        }
        
        RollingFileAppender<ILoggingEvent> outputStream = new RollingFileAppender<ILoggingEvent>();
        outputStream.setContext(loggerContext);
        outputStream.setFile(OUT_LOG_FILE);

        TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<ILoggingEvent>();
        policy.setContext(loggerContext);
        policy.setMaxHistory(OUT_LOG_RETENTION);
        policy.setFileNamePattern(OUT_LOG_FILE + ".%d{yyyy-MM-dd}");
        policy.setParent(outputStream);
        policy.start();
        outputStream.setRollingPolicy(policy);

        initOut(outputStream);
    }
    
    static void initOut(OutputStreamAppender<ILoggingEvent> outputStream) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.detachAndStopAllAppenders();
        
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%date [%thread] %-5level %logger (%file:%line\\) - %msg%n");
        encoder.start();
        
        outputStream.setEncoder(encoder);
        outputStream.start();

        rootLogger.addAppender(outputStream);
        rootLogger.setLevel(Level.toLevel(OUT_LOG_LEVEL));
        rootLogger.setAdditive(false);
    }

    static boolean noLogFile() {
        return OUT_LOG_FILE == null;
    }
    
    static void addShutdownHook(MQBroker broker) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    broker.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
    
}

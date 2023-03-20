/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.benchmark;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.srvutil.ServerUtil;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class RMQProducerPerf {
    //    private static DefaultMQProducer producer = null;
//    private static StatsBenchmarkProducer statsBenchmark = null;
    private byte[] msgBody;
    private final int MAX_LOAD_FACTOR = 20480;
    private final int SEND_THRESHOLD_INIT = 32;
    private AtomicInteger loadThreshold = new AtomicInteger(MAX_LOAD_FACTOR);
    private AtomicInteger currentLoadFactor = new AtomicInteger(0);
    private AtomicInteger sendThreshold = new AtomicInteger(SEND_THRESHOLD_INIT);
    private final int SLEEP_FOR_A_WHILE = 100;
    private AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        if (args.length != 2 || !args[0].equals("-c")) {
            System.out.println("Usage: RMQProducerPerf -c CONFIG.json");
            return;
        }
        System.setProperty("rocketmq.client.logRoot","/root/clientLog");
        String path = args[1];
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(path));
        RMQConf[] rmqConfs = gson.fromJson(reader, RMQConf[].class);
        System.out.println("Found " + rmqConfs.length + " rmqConf:");
        for (RMQConf conf: rmqConfs) {
            System.out.println(gson.toJson(conf));
        }
        StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();
        ExecutorService sendThreadPool = Executors.newFixedThreadPool(rmqConfs.length);
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());
        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();
        executorService.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    doPrintStats(snapshotList,  statsBenchmark, false);
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
        }, 10000, 10000, TimeUnit.MILLISECONDS);

        for (int i=0; i<rmqConfs.length; i++) {
            RMQConf conf = rmqConfs[i];
            String[] subArgs = toArgs(conf);
            StringBuilder sb = new StringBuilder();
            for (String arg: subArgs) {
                sb.append(arg + " ");
            }
            System.out.println("subArgs: " + sb.toString());
            String producerGroup = "producer_benchmark_" + i;
            int finalI = i;
            sendThreadPool.execute(() -> {
                try {
                    RMQProducerPerf rmqProducerPerf = new RMQProducerPerf();
                    rmqProducerPerf.start(subArgs, statsBenchmark, producerGroup, finalI);
                } catch (MQClientException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        sendThreadPool.shutdown();
        sendThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        executorService.shutdown();
        try {
            executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        if (snapshotList.size() > 1) {
            doPrintStats(snapshotList, statsBenchmark, true);
        } else {
            System.out.printf("[Complete] Send Total: %d Send Failed: %d Response Failed: %d%n",
                    statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
                    statsBenchmark.getSendRequestFailedCount().longValue(), statsBenchmark.getReceiveResponseFailedCount().longValue());
        }
    }

    private static String[] toArgs(RMQConf conf) {
        List<String> list = new ArrayList<>();
        if (conf.topic != null) {
            list.add("-t");
            list.add(conf.topic);
        }
        if (conf.topicCount != null) {
            list.add("-tc");
            list.add(conf.topicCount.toString());
        }
        if (conf.messageSize != null) {
            list.add("-s");
            list.add(conf.messageSize.toString());
        }
        if (conf.keyEnable != null) {
            list.add("-k");
            list.add(conf.keyEnable.toString());
        }
        if (conf.propertySize != null) {
            list.add("-p");
            list.add(conf.propertySize.toString());
        }
        if (conf.tagCount != null) {
            list.add("-l");
            list.add(conf.tagCount.toString());
        }
        if (conf.msgTraceEnable != null) {
            list.add("-m");
            list.add(conf.msgTraceEnable.toString());
        }
        if (conf.aclEnable != null) {
            list.add("-a");
            list.add(conf.aclEnable.toString());
        }
        if (conf.messageNum != null) {
            list.add("-q");
            list.add(conf.messageNum.toString());
        }
        if (conf.delayEnable != null) {
            list.add("-d");
            list.add(conf.delayEnable.toString());
        }
        if (conf.delayLevel != null) {
            list.add("-e");
            list.add(conf.delayLevel.toString());
        }
        if (conf.asyncEnable != null) {
            list.add("-y");
            list.add(conf.asyncEnable.toString());
        }
        if (conf.threadNum != null) {
            list.add("-w");
            list.add(conf.threadNum.toString());
        }
        if (conf.nameServer != null) {
            list.add("-n");
            list.add(conf.nameServer.toString());
        }
        return list.toArray(new String[0]);
    }

    public int getMaxLoadFacotr() {
        return MAX_LOAD_FACTOR;
    }

    public static int getLoadFactor(int msgSize) {
        return Math.max(msgSize >> 10, 4);
    }

    public void stop() {
        running.set(false);
    }

    public void start(String[] args, final StatsBenchmarkProducer statsBenchmark, String producerGroup, int id) throws MQClientException {
        start(args, null, statsBenchmark, producerGroup, id);
    }
    public void start(String[] args, DefaultMQProducer defaultMQProducer, final StatsBenchmarkProducer statsBenchmark, String producerGroup, int id) throws MQClientException {
        System.setProperty(RemotingCommand.SERIALIZE_TYPE_PROPERTY, SerializeType.ROCKETMQ.name());

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("RMQProducerPerf", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int topicCount = commandLine.hasOption("tc") ? Integer.parseInt(commandLine.getOptionValue("tc")) : 1;
        final int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
        final boolean keyEnable = commandLine.hasOption('k') && Boolean.parseBoolean(commandLine.getOptionValue('k'));
        final int propertySize = commandLine.hasOption('p') ? Integer.parseInt(commandLine.getOptionValue('p')) : 0;
        final int tagCount = commandLine.hasOption('l') ? Integer.parseInt(commandLine.getOptionValue('l')) : 0;
        final boolean msgTraceEnable = commandLine.hasOption('m') && Boolean.parseBoolean(commandLine.getOptionValue('m'));
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));
        final long messageNum = commandLine.hasOption('q') ? Long.parseLong(commandLine.getOptionValue('q')) : 0;
        final boolean delayEnable = commandLine.hasOption('d') && Boolean.parseBoolean(commandLine.getOptionValue('d'));
        final int delayLevel = commandLine.hasOption('e') ? Integer.parseInt(commandLine.getOptionValue('e')) : 1;
        final boolean asyncEnable = commandLine.hasOption('y') && Boolean.parseBoolean(commandLine.getOptionValue('y'));
        final int threadNum = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 4;
        final List<String> topicList = new ArrayList<>();
        if (topicCount > 1) {
            for (int i=0; i<topicCount; i++) {
                int numberOfDigits = SLMathUtil.getNumberOfDigits(topicCount);
//            String format = String.format("%%s%%0%dd", numberOfDigits);
                String format = "%s%d";
                topicList.add(String.format(format, topic, i));
            }
        } else {
            topicList.add(topic);
        }

        System.out.printf("topic: %s topicCount: %5d threadNum: %d messageSize: %d keyEnable: %b propertySize: %d tagCount: %d " +
                        "traceEnable: %b aclEnable: %b messageQuantity: %d%ndelayEnable: %b delayLevel: %s%n" +
                        "asyncEnable: %b%n",
                topic, topicCount, threadNum, messageSize, keyEnable, propertySize, tagCount, msgTraceEnable, aclEnable, messageNum,
                delayEnable, delayLevel, asyncEnable);

        StringBuilder sb = new StringBuilder(messageSize);
        for (int i = 0; i < messageSize; i++) {
            sb.append(RandomStringUtils.randomAlphanumeric(1));
        }
        msgBody = sb.toString().getBytes(StandardCharsets.UTF_8);

        final InternalLogger log = ClientLogger.getLog();

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadNum);

        final long[] msgNums = new long[threadNum];

        if (messageNum > 0) {
            Arrays.fill(msgNums, messageNum / threadNum);
            long mod = messageNum % threadNum;
            if (mod > 0) {
                msgNums[0] += mod;
            }
        }

        RPCHook rpcHook = null;
        if (aclEnable) {
            String ak = commandLine.hasOption("ak") ? String.valueOf(commandLine.getOptionValue("ak")) : AclClient.ACL_ACCESS_KEY;
            String sk = commandLine.hasOption("sk") ? String.valueOf(commandLine.getOptionValue("sk")) : AclClient.ACL_SECRET_KEY;
            rpcHook = AclClient.getAclRPCHook(ak, sk);
        }
        final DefaultMQProducer producer = defaultMQProducer != null ? defaultMQProducer : new DefaultMQProducer(producerGroup, rpcHook, msgTraceEnable, null);

        producer.setInstanceName(Long.toString(System.currentTimeMillis() + id));

        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }

        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);

        producer.start();

        for (int i = 0; i < threadNum; i++) {
            final long msgNumLimit = msgNums[i];
            if (messageNum > 0 && msgNumLimit == 0) {
                break;
            }
            final String topicThisThread = topicList.get(i % topicCount);
//            final String topicThisThread = topicList.get(i);
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    int num = 0;
                    AtomicInteger successCounter = new AtomicInteger(0);
                    while (running.get()) {
                        try {
                            final Message msg = buildMessage(topicThisThread);
                            final long beginTimestamp = System.currentTimeMillis();
                            if (keyEnable) {
                                msg.setKeys(String.valueOf(beginTimestamp / 1000));
                            }
                            if (delayEnable) {
                                msg.setDelayTimeLevel(delayLevel);
                            }
                            if (tagCount > 0) {
                                msg.setTags(String.format("tag%d", System.currentTimeMillis() % tagCount));
                            }
                            if (propertySize > 0) {
                                if (msg.getProperties() != null) {
                                    msg.getProperties().clear();
                                }
                                int i = 0;
                                int startValue = (new Random(System.currentTimeMillis())).nextInt(100);
                                int size = 0;
                                while (true) {
                                    String prop1 = "prop" + i, prop1V = "hello" + startValue;
                                    String prop2 = "prop" + (i + 1), prop2V = String.valueOf(startValue);
                                    msg.putUserProperty(prop1, prop1V);
                                    msg.putUserProperty(prop2, prop2V);
                                    size += prop1.length() + prop2.length() + prop1V.length() + prop2V.length();
                                    if (size > propertySize) {
                                        break;
                                    }
                                    i += 2;
                                    startValue += 2;
                                }
                            }
                            if (asyncEnable) {
                                ThreadPoolExecutor e = (ThreadPoolExecutor) producer.getDefaultMQProducerImpl().getAsyncSenderExecutor();
                                // Flow control
                                int factor = getLoadFactor(messageSize);
                                while (currentLoadFactor.get() + factor > loadThreshold.get()) {
                                    if (loadThreshold.get() < factor) {
                                        loadThreshold.set(factor);
                                        break;
                                    }
                                    Thread.sleep(SLEEP_FOR_A_WHILE);
//                                    System.out.println("sleep for a while");
//                                    log.info(String.format("sleep for a while, currentLoadFactor = %d, factor = %d, loadThreshold = %d", x, factor, y));
                                }
                                currentLoadFactor.addAndGet(factor);
                                producer.send(msg, new SendCallback() {
                                    @Override
                                    public void onSuccess(SendResult sendResult) {
                                        updateStatsSuccess(statsBenchmark, beginTimestamp);
                                        currentLoadFactor.addAndGet(-factor);
                                        if (sendResult.getSendStatus() != SendStatus.SEND_OK_FLOW_CONTROL) {
                                            int count = successCounter.incrementAndGet();
                                            log.info("send success successCounter set to" + count);
                                            if (count == sendThreshold.get()) {
                                                successCounter.set(0);
                                                if (loadThreshold.get() + factor < MAX_LOAD_FACTOR) {
                                                    loadThreshold.addAndGet(factor);
                                                    log.info("increase loadThreshold from " + (loadThreshold.get() - factor) + " to " + loadThreshold.get());
                                                }
                                            }
                                        } else {
                                            log.info("send success and in flow control");
                                            successCounter.set(0);
                                        }

                                    }

                                    @Override
                                    public void onException(Throwable e) {
//                                        log.info("here " + e.toString());
                                        statsBenchmark.getSendRequestFailedCount().increment();
                                        currentLoadFactor.addAndGet(-factor);
                                        loadThreshold.set(loadThreshold.get() >> 1);
//                                        loadThreshold.set(currentLoadFactor.addAndGet(-factor));
                                        successCounter.set(0);
                                        log.info(String.format("On exception, loadThreshold set to %d", loadThreshold.get()));
                                    }
                                });
                            } else {
                                producer.send(msg);
                                updateStatsSuccess(statsBenchmark, beginTimestamp);
                            }
                        } catch (RemotingException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);

                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        } catch (InterruptedException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                            }
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQBrokerException e) {
                            statsBenchmark.getReceiveResponseFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        }
                        if (messageNum > 0 && ++num >= msgNumLimit) {
                            break;
                        }
                    }
                }
            });
        }
        try {
            sendThreadPool.shutdown();
            sendThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
//            executorService.shutdown();
//            try {
//                executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//            }
//
//            if (snapshotList.size() > 1) {
//                doPrintStats(snapshotList, statsBenchmark, true);
//            } else {
//                System.out.printf("[Complete] Send Total: %d Send Failed: %d Response Failed: %d%n",
//                        statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
//                        statsBenchmark.getSendRequestFailedCount().longValue(), statsBenchmark.getReceiveResponseFailedCount().longValue());
//            }
            producer.shutdown();
        } catch (InterruptedException e) {
            log.error("[Exit] Thread Interrupted Exception", e);
        }
    }

    private static void updateStatsSuccess(StatsBenchmarkProducer statsBenchmark, long beginTimestamp) {
        statsBenchmark.getSendRequestSuccessCount().increment();
        statsBenchmark.getReceiveResponseSuccessCount().increment();
        final long currentRT = System.currentTimeMillis() - beginTimestamp;
        statsBenchmark.getSendMessageSuccessTimeTotal().add(currentRT);
        long prevMaxRT = statsBenchmark.getSendMessageMaxRT().longValue();
        while (currentRT > prevMaxRT) {
            boolean updated = statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
            if (updated)
                break;

            prevMaxRT = statsBenchmark.getSendMessageMaxRT().longValue();
        }
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("w", "threadNum", true, "Thread count, Default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "keyEnable", true, "Message Key Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "propertySize", true, "Property size, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("tc", "topicCount", true, "Topic Count, Default: 1");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("l", "tagCount", true, "Tag count, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "msgTraceEnable", true, "Message Trace Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "aclEnable", true, "Acl Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ak", "accessKey", true, "Acl access key, Default: 12345678");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sk", "secretKey", true, "Acl secret key, Default: rocketmq2");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "messageQuantity", true, "Send message quantity, Default: 0, running forever");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "delayEnable", true, "Delay message Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "delayLevel", true, "Delay message level, Default: 1");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("y", "asyncEnable", true, "Enable async produce, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private Message buildMessage(final String topic) {
        return new Message(topic, msgBody);
    }

    private static void doPrintStats(final LinkedList<Long[]> snapshotList, final StatsBenchmarkProducer statsBenchmark, boolean done) {
        Long[] begin = snapshotList.getFirst();
        Long[] end = snapshotList.getLast();

        final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
        final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

        if (done) {
            System.out.printf("[Complete] Send Total: %d Send TPS: %d Max RT(ms): %d Average RT(ms): %7.3f Send Failed: %d Response Failed: %d%n",
                    statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
                    sendTps, statsBenchmark.getSendMessageMaxRT().longValue(), averageRT, end[2], end[4]);
        } else {
            System.out.printf("Current Time: %s Send TPS: %d Max RT(ms): %d Average RT(ms): %7.3f Send Failed: %d Response Failed: %d%n",
                    UtilAll.timeMillisToHumanString2(System.currentTimeMillis()), sendTps, statsBenchmark.getSendMessageMaxRT().longValue(), averageRT, end[2], end[4]);
        }
    }
}



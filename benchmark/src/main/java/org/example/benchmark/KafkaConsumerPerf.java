package org.example.benchmark;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumerPerf {
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        if (args.length != 2 || !args[0].equals("-c")) {
            System.out.println("Usage: KafkaConsumerPerf -c CONFIG.json");
            return;
        }
//        System.setProperty("rocketmq.client.logRoot","/root/clientLog");
        String path = args[1];
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(path));
        KafkaConf[] kafkaConfs = gson.fromJson(reader, KafkaConf[].class);
        System.out.println("Found " + kafkaConfs.length + " kafkaConfs:");
        for (KafkaConf conf: kafkaConfs) {
            System.out.println(gson.toJson(conf));
        }
        StatsBenchmarkConsumer statsBenchmark = new StatsBenchmarkConsumer();
        ExecutorService consumeThreadPool = Executors.newFixedThreadPool(kafkaConfs.length);
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

        for (int i=0; i<kafkaConfs.length; i++) {
            KafkaConf conf = kafkaConfs[i];
            String[] subArgs = toArgs(conf);
            StringBuilder sb = new StringBuilder();
            for (String arg: subArgs) {
                sb.append(arg).append(" ");
            }
            System.out.println("subArgs: " + sb.toString());
            String consumerGroup = "producer_benchmark_" + i;
            int finalI = i;
            consumeThreadPool.execute(() -> {
                try {
                    KafkaConsumerPerf kafkaConsumerPerf = new KafkaConsumerPerf();
                    kafkaConsumerPerf.start(subArgs, statsBenchmark, consumerGroup, finalI);
                } catch (MQClientException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        consumeThreadPool.shutdown();
        consumeThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
//        } catch (InterruptedException e) {
//        }
    }

    private void start(String[] subArgs, StatsBenchmarkConsumer statsBenchmarkConsumer, String consumerGroup, int instanceId) throws MQClientException, IOException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("KafkaConsumerPerf", subArgs, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadNum = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 1;
        final String bootstrapServer = commandLine.hasOption('b') ? commandLine.getOptionValue('b').trim() : null;


        System.out.printf("topic: %s, threadNum %d, group: %s\n",
                topic, threadNum, consumerGroup);

        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName() + instanceId);
        config.put("group.id", consumerGroup);
        if (bootstrapServer == null) {
            System.out.println("need bootstrap.servers");
            return;
        }
        config.put("bootstrap.servers", bootstrapServer);
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("auto.offset.reset", "earliest");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Consumer Started.");
        while (true) {
            Duration duration = Duration.ofMillis(1000);
            ConsumerRecords<byte[], byte[]> records = consumer.poll(duration);
            records.forEach(record -> {
                long now = System.currentTimeMillis();

                statsBenchmarkConsumer.getReceiveMessageTotalCount().increment();

                long born2ConsumerRT = 0;
                statsBenchmarkConsumer.getBorn2ConsumerTotalRT().add(born2ConsumerRT);

                long store2ConsumerRT = 0;
                statsBenchmarkConsumer.getStore2ConsumerTotalRT().add(store2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getBorn2ConsumerMaxRT(), born2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getStore2ConsumerMaxRT(), store2ConsumerRT);
            });
        }
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("tc", "topicCount", true, "Topic Count, Default: 1");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "threadNum", true, "Thread count, Default: 20");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupPrefix", true, "Consumer group name, Default: benchmark_consumer");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("p", "group prefix enable", true, "Is group prefix enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "filterType", true, "TAG, SQL92");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "expression", true, "filter expression content file path.ie: ./test/expr");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "fail rate", true, "consumer fail rate, default 0");
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

        opt = new Option("b", "bootstrapServer", true, "Kafka bootstrapServer");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    public static void compareAndSetMax(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                break;

            prev = target.get();
        }
    }

    private static String[] toArgs(KafkaConf conf) {
        List<String> list = new ArrayList<>();
        if (conf.topic != null) {
            list.add("--topic");
            list.add(conf.topic);
        }
        if (conf.topicNum != null) {
            list.add("--topic-nums");
            list.add(conf.topicNum.toString());
        }
        if (conf.messageNum != null) {
            list.add("-n");
            list.add(conf.messageNum.toString());
        }
        if (conf.messageSize != null) {
            list.add("-s");
            list.add(conf.messageSize.toString());
        }
        if (conf.producerProps != null) {
            list.add("--producer-props");
            list.addAll(conf.producerProps);
        }
        if (conf.producerConfig != null) {
            list.add("--producer.config");
            list.add(conf.producerConfig.toString());
        }
        if (conf.threadNum != null) {
            list.add("-w");
            list.add(conf.threadNum.toString());
        }
        if (conf.asyncEnable != null) {
            list.add("--asyncEnable");
            list.add(conf.asyncEnable.toString());
        }
        if (conf.bootstrapServer != null) {
            list.add("-b");
            list.add(conf.bootstrapServer.toString());
        }
        return list.toArray(new String[0]);
    }

    private static void doPrintStats(final LinkedList<Long[]> snapshotList, final StatsBenchmarkConsumer statsBenchmarkConsumer, boolean done) {
        if (snapshotList.size() >= 10) {
            Long[] begin = snapshotList.getFirst();
            Long[] end = snapshotList.getLast();

            final long consumeTps =
                    (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
            final double averageB2CRT = (end[2] - begin[2]) / (double) (end[1] - begin[1]);
            final double averageS2CRT = (end[3] - begin[3]) / (double) (end[1] - begin[1]);
            final long failCount = end[4] - begin[4];
            final long b2cMax = statsBenchmarkConsumer.getBorn2ConsumerMaxRT().get();
            final long s2cMax = statsBenchmarkConsumer.getStore2ConsumerMaxRT().get();

            statsBenchmarkConsumer.getBorn2ConsumerMaxRT().set(0);
            statsBenchmarkConsumer.getStore2ConsumerMaxRT().set(0);

            System.out.printf("Current Time: %s TPS: %d FAIL: %d AVG(B2C) RT(ms): %7.3f AVG(S2C) RT(ms): %7.3f MAX(B2C) RT(ms): %d MAX(S2C) RT(ms): %d%n",
                    System.currentTimeMillis(), consumeTps, failCount, averageB2CRT, averageS2CRT, b2cMax, s2cMax
            );
        }
    }
}



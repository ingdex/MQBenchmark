package org.example.benchmark;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.srvutil.ServerUtil.buildCommandlineOptions;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class KafkaProducerPerf {
    private org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer;
//    private static StatsBenchmarkProducer statsBenchmark = null;
    private static AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        if (args.length != 2 || !args[0].equals("-c")) {
            System.out.println("Usage: kafkaproducer -c CONFIG.json");
            return;
        }
        String path = args[1];
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(path));
        KafkaConf[] kafkaConfs = gson.fromJson(reader, KafkaConf[].class);
        System.out.println("Found " + kafkaConfs.length + " kafkaConf:");
        for (KafkaConf conf: kafkaConfs) {
            System.out.println(gson.toJson(conf));
        }
        StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();
        ExecutorService sendThreadPool = Executors.newFixedThreadPool(kafkaConfs.length);
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
                sb.append(arg + " ");
            }
            System.out.println("subArgs: " + sb.toString());
            String producerGroup = "producer_benchmark_" + i;
            sendThreadPool.execute(() -> {
                try {
                    KafkaProducerPerf kafkaProducerPerf = new KafkaProducerPerf();
                    kafkaProducerPerf.start(subArgs, statsBenchmark);
                } catch (Exception e) {
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
            list.add(conf.producerProps.toString());
        }
        if (conf.producerConfig != null) {
            list.add("--producer.config");
            list.add(conf.producerConfig.toString());
        }
//        if (conf.payloadFilePath != null) {
//            list.add("-l");
//            list.add(conf.payloadFilePath.toString());
//        }
//        if (conf.transactionalId != null) {
//            list.add("-m");
//            list.add(conf.transactionalId.toString());
//        }
//        if (conf.shouldPrIntegerMetrics != null) {
//            list.add("-a");
//            list.add(conf.shouldPrIntegerMetrics.toString());
//        }
//        if (conf.transactionDurationMs != null) {
//            list.add("-q");
//            list.add(conf.transactionDurationMs.toString());
//        }
        if (conf.threadNum != null) {
            list.add("-w");
            list.add(conf.threadNum.toString());
        }
        if (conf.asyncEnable != null) {
            list.add("--asyncEnable");
            list.add(conf.asyncEnable.toString());
        }
        return list.toArray(new String[0]);
    }

    public void setProducer(org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer) {
        this.producer = producer;
    }

//    public static void setStatsBenchmark(StatsBenchmarkProducer statsBenchmark) {
//        KafkaProducerPerf.statsBenchmark = statsBenchmark;
//    }

    public static void stop() {
        running.set(false);
    }

    public void start(String[] args, StatsBenchmarkProducer statsBenchmark) throws Exception {
        ArgumentParser parser = argParser();
        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            final String topic = res.getString("topic");
            final int topicNum = res.getInt("topicNum");
            final long messageNum = res.getLong("messageNum");
            final Integer messageSize = res.getInt("messageSize");
            final List<String> producerProps = res.getList("producerConfig");
            final String producerConfig = res.getString("producerConfigFile");
            final String payloadFilePath = res.getString("payloadFile");
            final String transactionalId = res.getString("transactionalId");
            final boolean shouldPrintMetrics = res.getBoolean("printMetrics");
            final long transactionDurationMs = res.getLong("transactionDurationMs");
            final boolean transactionsEnabled = 0 < transactionDurationMs;
            final int threadNum = res.getInt("threadNum");
            final boolean asyncEnable = res.getBoolean("asyncEnable");
            final List<String> topicList = new ArrayList<>();
            if (topicNum > 1) {
                for (int i = 0; i < topicNum; i++) {
                    int numberOfDigits = SLMathUtil.getNumberOfDigits(topicNum);
                    String format = String.format("%%s%%0%dd", numberOfDigits);
                    topicList.add(String.format(format, topic, i));
                }
            } else {
                topicList.add(topic);
            }
            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");


            System.out.printf("topic: %s topicNum: %d threadNum: %d messageNum %d messageSize: %d producerProps: %s producerConfig: %s payloadFilePath: %s " +
                            "transactionalId: %s shouldPrintMetrics: %b transactionDurationMs: %d%n transactionsEnabled: %b" +
                            "asyncEnable: %b%n",
                    topic, topicNum, threadNum, messageNum, messageSize, producerProps, producerConfig, payloadFilePath, transactionalId, shouldPrintMetrics, transactionDurationMs,
                    transactionsEnabled, asyncEnable);


            if (producerProps == null && producerConfig == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }

            List<byte[]> payloadByteList = readPayloadFile(payloadFilePath, payloadDelimiter);

            Properties props = readProps(producerProps, producerConfig, transactionalId, transactionsEnabled);

            if (producer == null) {
                producer = createKafkaProducer(props);
            }

            if (transactionsEnabled)
                producer.initTransactions();

            /* setup perf test */
            Random random = new Random(0);

            final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadNum);

            final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

            final long[] msgNums = new long[threadNum];

            if (messageNum > 0) {
                Arrays.fill(msgNums, messageNum / threadNum);
                long mod = messageNum % threadNum;
                if (mod > 0) {
                    msgNums[0] += mod;
                }
            }

            final byte[] payload = generateRandomPayload(messageSize, payloadByteList, random);

            // no transaction!!!
            for (int i = 0; i < threadNum; i++) {
                final long msgNumLimit = msgNums[i];
                if (messageNum > 0 && msgNumLimit == 0) {
                    break;
                }
                int threadPerTopic = threadNum / topicNum;
                if (threadPerTopic == 0) {
                    threadPerTopic = 1;
                }
                final String topicThisThread = topicList.get(i / threadPerTopic);
                sendThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        int num = 0;
                        while (running.get()) {
                            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicThisThread, payload);
                            long beginTimestamp = System.currentTimeMillis();
                            if (asyncEnable) {
                                producer.send(record, new Callback() {
                                    @Override
                                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                        if (e != null) {
                                            statsBenchmark.getSendRequestFailedCount().increment();
                                            e.printStackTrace();
                                            return;
                                        }
                                        updateStatsSuccess(statsBenchmark, beginTimestamp);
                                    }
                                });
                            } else {
                                try {
                                    producer.send(record).get();
                                } catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                                break;
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
//                executorService.shutdown();
//                executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                if (snapshotList.size() > 1) {
                    doPrintStats(snapshotList, statsBenchmark, true);
                } else {
                    System.out.printf("[Complete] Send Total: %d Send Failed: %d Response Failed: %d%n",
                            statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
                            statsBenchmark.getSendRequestFailedCount().longValue(), statsBenchmark.getReceiveResponseFailedCount().longValue());
                }
                producer.flush();
                producer.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
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

    static org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    static byte[] generateRandomPayload(Integer messageSize, List<byte[]> payloadByteList,
                                        Random random) {
        byte[] payload = new byte[messageSize];
        for (int j = 0; j < payload.length; ++j)
            payload[j] = (byte) (random.nextInt(26) + 65);
        return payload;
    }

    static Properties readProps(List<String> producerProps, String producerConfig, String transactionalId,
                                boolean transactionsEnabled) throws IOException {
        Properties props = new Properties();
        if (producerConfig != null) {
            props.putAll(Utils.loadProps(producerConfig));
        }
        if (producerProps != null)
            for (String prop : producerProps) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                props.put(pieces[0], pieces[1]);
            }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (transactionsEnabled) props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "perf-producer-client");
        }
        return props;
    }

    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            System.out.println("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0) {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            System.out.println("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        return payloadByteList;
    }

    /**
     * Get the command-line argument parser.
     */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        parser.addArgument("--topic", "-t")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-messages", "-n")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-MESSAGES")
                .dest("messageNum")
                .help("number of messages to produce");

        parser.addArgument("--topic-nums")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("NUM-TOPICS")
                .dest("topicNum")
                .setDefault(4)
                .help("number of topics");

        payloadOptions.addArgument("--message-size", "-s")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("MESSAGE-SIZE")
                .dest("messageSize")
                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--producer-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("producerConfig")
                .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
                        "These configs take precedence over those passed via --producer.config.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("producer config properties file.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--transactional-id")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("TRANSACTIONAL-ID")
                .dest("transactionalId")
                .setDefault("performance-producer-default-transactional-id")
                .help("The transactionalId to use if transaction-duration-ms is > 0. Useful when testing the performance of concurrent transactions.");

        parser.addArgument("--print-metrics")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PRINT-METRICS")
                .dest("printMetrics")
                .help("print out metrics at the end of the test.");

        parser.addArgument("--transaction-duration-ms")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("TRANSACTION-DURATION")
                .dest("transactionDurationMs")
                .setDefault(0L)
                .help("The max age of each transaction. The commitTransaction will be called after this time has elapsed. Transactions are only enabled if this value is positive.");

        parser.addArgument("--threadNum", "-w")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("THREAD-COUNT")
                .dest("threadNum")
                .setDefault(4)
                .help("producer thread count");

        parser.addArgument("--asyncEnable", "-a")
                .action(store())
                .required(false)
                .type(Boolean.class)
                .metavar("ASYNC-ENABLE")
                .dest("asyncEnable")
                .setDefault(false)
                .help("producer is async");

        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long messageNum, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (messageNum / Math.min(messageNum, 500000));
            this.latencies = new int[(int) (messageNum / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
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

package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.benchmark.KafkaProducerPerf;
import org.example.benchmark.StatsBenchmarkProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.LongAdder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerTest {
    @Mock
    org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer;
    @Test
    public void testKafkaProducer() throws Exception {
        LongAdder totalMsg = new LongAdder();
//        RecordMetadata recordMetadata = new RecordMetadata();
//        doNothing().when(producer).initTransactions();
        doAnswer(invocationOnMock -> {
            totalMsg.increment();
            Callback callback = invocationOnMock.getArgument(1);
            callback.onCompletion(null, null);
            return null;
        }).when(producer).send(any(), any());
        doNothing().when(producer).flush();
        doNothing().when(producer).close();
        KafkaProducerPerf.setProducer(producer);
        StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();
        KafkaProducerPerf.setStatsBenchmark(statsBenchmark);
        new Thread(()->{
            try {
                Thread.sleep(40000);
                KafkaProducerPerf.stop();
                System.out.println("stopped");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        KafkaProducerPerf.start(new String[]{"--asyncEnable", "true", "--topic", "topic", "-s", "4096", "-n", "0", "--producer-props", "props=props"});

        System.out.println("statsBenchmark.getSendRequestSuccessCount()" + statsBenchmark.getSendRequestSuccessCount());
        System.out.println("totalMsg.longValue()" + totalMsg.longValue());
    }
}

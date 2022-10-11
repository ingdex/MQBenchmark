package org.example;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.example.benchmark.RMQProducerPerf;
import org.example.benchmark.StatsBenchmarkProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RMQProducerTest {
    @Mock
    DefaultMQProducer producer;
    @Mock
    DefaultMQProducerImpl defaultMQProducerImpl;
    @Mock
    ThreadPoolExecutor e;
    @Mock
    BlockingQueue<Runnable> workQue;
    @Mock
    SendResult sendResult;

    @Test
    public void testRMQProducer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        LongAdder totalMsg = new LongAdder();
        doNothing().when(producer).setInstanceName(anyString());
//        doNothing().when(producer).setNamesrvAddr(anyString());
        doNothing().when(producer).setCompressMsgBodyOverHowmuch(anyInt());
        doNothing().when(producer).start();
//        when(producer.getDefaultMQProducerImpl()).thenReturn(defaultMQProducerImpl);

//        when(defaultMQProducerImpl.getAsyncSenderExecutor()).thenReturn(e);
//        when(e.getQueue()).thenReturn(workQue);
//        doAnswer(invocation -> {
//            Random random = new Random();
//            if (random.nextInt(100) < 5) {
//                return RMQProducerPerf.getMaxLengthAsync()+1;
//            } else {
//                return 1;
//            }
//        }).when(workQue).size();
        doAnswer(invocationOnMock -> {
            totalMsg.increment();
            return sendResult;
        }).when(producer).send((Message) any());
//        doAnswer(invocationOnMock -> {
//            SendCallback sendCallback = invocationOnMock.getArgument(1);
//            totalMsg.increment();
//            sendCallback.onSuccess(sendResult);
//            return null;
//        }).when(producer).send((Message) any(), (SendCallback) any());
        doNothing().when(producer).shutdown();
        System.out.println("start");
        StatsBenchmarkProducer statsBenchmarkProducer = new StatsBenchmarkProducer();
        new Thread(() -> {
            try {
                Thread.sleep(10000);
                RMQProducerPerf.stop();
                System.out.println("stopped");
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }).start();
        RMQProducerPerf.start(new String[]{"RMQProducerPerf", "-t", "topic"}, producer, statsBenchmarkProducer, "producer_benchmark");

        assertEquals(statsBenchmarkProducer.getSendRequestSuccessCount().longValue(), totalMsg.longValue());
        System.out.println("statsBenchmarkProducer.getSendRequestSuccessCount().longValue(): " + statsBenchmarkProducer.getSendRequestSuccessCount().longValue());
        System.out.println("totalMsg.longValue(): " + totalMsg.longValue());
    }
}

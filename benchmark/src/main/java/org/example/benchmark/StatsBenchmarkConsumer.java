package org.example.benchmark;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

class StatsBenchmarkConsumer {
    private final LongAdder receiveMessageTotalCount = new LongAdder();

    private final LongAdder born2ConsumerTotalRT = new LongAdder();

    private final LongAdder store2ConsumerTotalRT = new LongAdder();

    private final AtomicLong born2ConsumerMaxRT = new AtomicLong(0L);

    private final AtomicLong store2ConsumerMaxRT = new AtomicLong(0L);

    private final LongAdder failCount = new LongAdder();

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
                System.currentTimeMillis(),
                this.receiveMessageTotalCount.longValue(),
                this.born2ConsumerTotalRT.longValue(),
                this.store2ConsumerTotalRT.longValue(),
                this.failCount.longValue()
        };

        return snap;
    }

    public LongAdder getReceiveMessageTotalCount() {
        return receiveMessageTotalCount;
    }

    public LongAdder getBorn2ConsumerTotalRT() {
        return born2ConsumerTotalRT;
    }

    public LongAdder getStore2ConsumerTotalRT() {
        return store2ConsumerTotalRT;
    }

    public AtomicLong getBorn2ConsumerMaxRT() {
        return born2ConsumerMaxRT;
    }

    public AtomicLong getStore2ConsumerMaxRT() {
        return store2ConsumerMaxRT;
    }

    public LongAdder getFailCount() {
        return failCount;
    }

}
package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkQueueListener implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkQueueListener.class);
    private static final int MAX_MESSAGES_PER_PULL = 2;
    private static final int NO_PULL_SLEEP_SECONDS = 5;

    private final Stairway stairway;
    private final Queue workQueue;
    private final int maxQueuedFlights;

    public WorkQueueListener(Stairway stairway, int maxQueuedFlights, Queue workQueue) {
        this.stairway = stairway;
        this.maxQueuedFlights = maxQueuedFlights;
        this.workQueue = workQueue;
    }

    // Notes on rate limiting what we take from the work queue
    //
    // In general, we want to treat all ways that run flights equally as far as the thread pool goes.
    // Whether a flight comes from the REST API or from the work queue, we want to allow it to run.
    //
    // The algorithm here will pull MAX_MESSAGES_PER_PULL from the queue and resume them, up to the
    // point where the threadPool queue depth is at or over its maxQueuedFlights size. When that happens
    // we sleep for NO_PULL_SLEEP_SECONDS and then check the queue depth.
    //
    // The reason to limit to MAX_MESSAGES_PER_PULL is that the evaluation of queue depth is done at, say,
    // time T0, but the pull will wait for message arrival from pubsub at, say, time T1. By time T1, the
    // queue depth situation might have changed. So we don't want to set up to grab lots of messages and
    // then find we are queuing flights way over maxQueuedFlights.
    @Override
    public void run() {
        try {
            while (!stairway.isQuietingDown()) {
                ThreadPoolExecutor threadPool = stairway.getThreadPool();
                int queueDepth = threadPool.getQueue().size();
                if (queueDepth < maxQueuedFlights) {
                    logger.info("Asking the work queue for messages: " + MAX_MESSAGES_PER_PULL);
                    workQueue.dispatchMessages(MAX_MESSAGES_PER_PULL, QueueMessage::processMessage);
                } else {
                    // No room to queue messages. Take a rest.
                    TimeUnit.SECONDS.sleep(NO_PULL_SLEEP_SECONDS);
                }
            }
        } catch(InterruptedException ex){
            logger.info("WorkQueueListener interrupted - exiting");
        }
    }
}

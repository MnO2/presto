/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.server.DownstreamStatsRequest;
import com.facebook.presto.server.NodeStatusNotificationManager;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_BUFFER_CLOSE_FAILED;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class PageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(PageBufferClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        boolean addPages(PageBufferClient client, List<SerializedPage> pages);

        void requestComplete(PageBufferClient client);

        void clientFinished(PageBufferClient client);

        void clientFailed(PageBufferClient client, Throwable cause);

        long getBufferRetainedSizeInBytes();
        void clientNodeShutdown(PageBufferClient client);
    }

    private final RpcShuffleClient resultClient;
    private final boolean acknowledgePages;
    private final URI location;
    private final ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Backoff backoff;
    private final Queue<Long> clientGetSentTimes = new ConcurrentLinkedQueue<>();
    private final Queue<Long> clientGetResponseCalledTimes = new ConcurrentLinkedQueue<>();

    private final Queue<Long> clientDeleteSentTimes = new ConcurrentLinkedQueue<>();
    private final Queue<Long> clientDeleteResponseCalledTimes = new ConcurrentLinkedQueue<>();
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private ListenableFuture<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private String taskInstanceId;
    @GuardedBy("this")
    private boolean isServerGracefulShutdown;

    private boolean isFastDraining;
    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();
    private final Queue<UpstreamOutputBufferStats> upstreamBufferedBytes = new ConcurrentLinkedQueue<>();

    private final Executor pageBufferClientCallbackExecutor;
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final NodeStatusNotificationManager nodeStatusNotifier;
    private Optional<Long> lastRequestScheduledTime = Optional.empty();
    private final Queue<Long> delayNanoSeconds = new ConcurrentLinkedQueue<>();

    public PageBufferClient(
            RpcShuffleClient resultClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor pageBufferClientCallbackExecutor,
            NodeStatusNotificationManager nodeStatusNotifier)
    {
        this(resultClient, maxErrorDuration, acknowledgePages, location, clientCallback, scheduler, Ticker.systemTicker(), pageBufferClientCallbackExecutor, nodeStatusNotifier);
    }

    public PageBufferClient(
            RpcShuffleClient resultClient,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            URI location,
            ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor,
            NodeStatusNotificationManager nodeStatusNotifier)
    {
        this.resultClient = requireNonNull(resultClient, "resultClient is null");
        this.acknowledgePages = acknowledgePages;
        this.location = requireNonNull(location, "location is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(maxErrorDuration, ticker);
        this.nodeStatusNotifier = nodeStatusNotifier;

        try {
            InetAddress address = Inet6Address.getByName(location.getHost());
            this.nodeStatusNotifier.getNotificationProvider().registerRemoteHostShutdownEventListener(address, this::onWorkerNodeShutdown);
        }
        catch (UnknownHostException exception) {
            log.error("Unable to parse URI location's host address into IP, skipping registerGracefulShutdownEventListener.");
        }
    }

    private synchronized void onWorkerNodeShutdown()
    {
        setFastDraining();
        clientCallback.clientNodeShutdown(this);
    }

    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (future != null) {
            state = "running";
        }
        else if (scheduled) {
            state = "scheduled";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }

        long rejectedRows = rowsRejected.get();
        int rejectedPages = pagesRejected.get();

        return new PageBufferClientStatus(
                location,
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
                rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                future == null ? "not scheduled" : "processing request");
    }

    public synchronized boolean isRunning()
    {
        return future != null;
    }

    public boolean isServerGracefulShutdown()
    {
        return isServerGracefulShutdown;
    }

    public void setFastDraining()
    {
        isFastDraining = true;
    }

    public boolean isFastDraining()
    {
        return isFastDraining;
    }

    public List<UpstreamOutputBufferStats> getUpstreamOutputBufferStats()
    {
        return upstreamBufferedBytes.stream().collect(Collectors.toList());
    }

    @Override
    public void close()
    {
        boolean shouldSendDelete;
        Future<?> future;
        synchronized (this) {
            shouldSendDelete = !closed;

            closed = true;

            future = this.future;

            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null && !future.isDone()) {
            // do not terminate if the request is already running to avoid closing pooled connections
            future.cancel(false);
        }

        // abort the output buffer on the remote node; response of delete is ignored
        if (shouldSendDelete) {
            sendDelete();
        }
    }

    public synchronized void scheduleRequest(DataSize maxResponseSize)
    {
        if (closed || (future != null) || scheduled) {
            return;
        }
        scheduled = true;

        // start before scheduling to include error delay
        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        scheduler.schedule(() -> {
            try {
                initiateRequest(maxResponseSize);
            }
            catch (Throwable t) {
                // should not happen, but be safe and fail the operator
                clientCallback.clientFailed(PageBufferClient.this, t);
            }
        }, delayNanos, NANOSECONDS);

        while (delayNanoSeconds.size() >= 10) {
            delayNanoSeconds.poll();
        }
        delayNanoSeconds.add(delayNanos);

        lastRequestScheduledTime = Optional.of(System.nanoTime());

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    public List<Long> getDelayNanos()
    {
        return delayNanoSeconds.stream().collect(Collectors.toList());
    }

    public Optional<Duration> getLastRequestScheduledTime()
    {
        if (lastRequestScheduledTime.isPresent()) {
            return Optional.of(Duration.nanosSince(lastRequestScheduledTime.get()));
        }
        else {
            return Optional.empty();
        }
    }

    private synchronized void initiateRequest(DataSize maxResponseSize)
    {
        scheduled = false;
        if (closed || (future != null)) {
            return;
        }

        if (completed) {
            sendDelete();
        }
        else {
            sendGetResults(maxResponseSize);
        }

        lastUpdate = DateTime.now();
    }

    public void startDownstreamStatsPeriodicUpdate()
    {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                String[] paths = location.getPath().split("/");
                OutputBuffers.OutputBufferId outputBufferId = OutputBuffers.OutputBufferId.fromString(paths[paths.length - 1]);
                MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

                long heapMemoryUsed = memoryUsage.getUsed();
                long bufferRetainedSizeInBytes = clientCallback.getBufferRetainedSizeInBytes();
                DownstreamStatsRequest downstreamStatsRequest = new DownstreamStatsRequest(outputBufferId,
                        heapMemoryUsed,
                        bufferRetainedSizeInBytes,
                        clientGetSentTimes.stream().collect(Collectors.toList()),
                        clientGetResponseCalledTimes.stream().collect(Collectors.toList()),
                        clientDeleteSentTimes.stream().collect(Collectors.toList()),
                        clientDeleteResponseCalledTimes.stream().collect(Collectors.toList()));
                sendDownstreamStats(downstreamStatsRequest);
            }
            catch (Throwable t) {
                log.error(t);
            }
        }, 5, 5, SECONDS);
    }

    public void sendDownstreamStats(DownstreamStatsRequest downstreamStatsRequest)
    {
        resultClient.sendDownstreamStats(downstreamStatsRequest);
    }

    private synchronized void sendGetResults(DataSize maxResponseSize)
    {
        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(token)).build();

        ListenableFuture<PagesResponse> resultFuture = resultClient.getResults(token, maxResponseSize);

        clientGetSentTimes.add(System.currentTimeMillis());
        while (clientGetSentTimes.size() > 5) {
            clientGetSentTimes.poll();
        }

        future = resultFuture;
        Futures.addCallback(resultFuture, new FutureCallback<PagesResponse>()
        {
            @Override
            public void onSuccess(PagesResponse result)
            {
                checkNotHoldsLock(this);

                clientGetResponseCalledTimes.add(System.currentTimeMillis());
                while (clientGetResponseCalledTimes.size() > 5) {
                    clientGetResponseCalledTimes.poll();
                }

                backoff.success();

                List<SerializedPage> pages;
                boolean pagesAccepted;
                try {
                    boolean shouldAcknowledge = false;
                    synchronized (PageBufferClient.this) {
                        if (taskInstanceId == null) {
                            taskInstanceId = result.getTaskInstanceId();
                        }

                        if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                            // TODO: update error message
                            throw new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, fromUri(uri)));
                        }

                        if (result.getToken() == token) {
                            pages = result.getPages();
                            token = result.getNextToken();
                            shouldAcknowledge = pages.size() > 0;
                        }
                        else {
                            pages = ImmutableList.of();
                        }
                    }

                    if (shouldAcknowledge && acknowledgePages) {
                        // Acknowledge token without handling the response.
                        // The next request will also make sure the token is acknowledged.
                        // This is to fast release the pages on the buffer side.
                        resultClient.acknowledgeResultsAsync(result.getNextToken());
                    }

                    for (SerializedPage page : pages) {
                        if (!isChecksumValid(page)) {
                            throw new PrestoException(SERIALIZED_PAGE_CHECKSUM_ERROR, format("Received corrupted serialized page from host %s", HostAddress.fromUri(uri)));
                        }
                    }

                    // add pages:
                    // addPages must be called regardless of whether pages is an empty list because
                    // clientCallback can keep stats of requests and responses. For example, it may
                    // keep track of how often a client returns empty response and adjust request
                    // frequency or buffer size.
                    pagesAccepted = clientCallback.addPages(PageBufferClient.this, pages);
                }
                catch (PrestoException e) {
                    handleFailure(e, resultFuture);
                    return;
                }

                // update client stats
                if (!pages.isEmpty()) {
                    int pageCount = pages.size();
                    long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
                    if (pagesAccepted) {
                        pagesReceived.addAndGet(pageCount);
                        rowsReceived.addAndGet(rowCount);
                    }
                    else {
                        pagesRejected.addAndGet(pageCount);
                        rowsRejected.addAndGet(rowCount);
                    }
                }

                while (upstreamBufferedBytes.size() >= 10) {
                    upstreamBufferedBytes.poll();
                }
                upstreamBufferedBytes.add(new UpstreamOutputBufferStats(location.toString(), result.getBufferedBytes(), System.currentTimeMillis()));
                requestsCompleted.incrementAndGet();

                synchronized (PageBufferClient.this) {
                    if (result.isServerGracefulShutdown()) {
                        isServerGracefulShutdown = true;
                    }

                    // client is complete, acknowledge it by sending it a delete in the next request
                    if (result.isClientComplete()) {
                        completed = true;
                    }

                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                clientCallback.requestComplete(PageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug("Request to %s failed %s", uri, t);
                checkNotHoldsLock(this);

                clientGetResponseCalledTimes.add(System.currentTimeMillis());
                while (clientGetResponseCalledTimes.size() > 5) {
                    clientGetResponseCalledTimes.poll();
                }

                t = resultClient.rewriteException(t);
                if (!(t instanceof PrestoException) && backoff.failure()) {
                    String message = format("%s (%s - %s failures, failure duration %s, total failed request time %s)",
                            WORKER_NODE_ERROR,
                            uri,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    t = new PageTransportTimeoutException(fromUri(uri), message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, pageBufferClientCallbackExecutor);
    }

    private synchronized void sendDelete()
    {
        ListenableFuture<?> resultFuture = resultClient.abortResults();
        future = resultFuture;

        clientDeleteSentTimes.add(System.currentTimeMillis());
        while (clientDeleteSentTimes.size() > 5) {
            clientDeleteSentTimes.poll();
        }
        Futures.addCallback(resultFuture, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(@Nullable Object result)
            {
                checkNotHoldsLock(this);
                clientDeleteResponseCalledTimes.add(System.currentTimeMillis());
                while (clientDeleteResponseCalledTimes.size() > 5) {
                    clientDeleteResponseCalledTimes.poll();
                }
                backoff.success();
                synchronized (PageBufferClient.this) {
                    closed = true;
                    if (future == resultFuture) {
                        future = null;
                    }
                    lastUpdate = DateTime.now();
                }
                requestsCompleted.incrementAndGet();
                clientCallback.clientFinished(PageBufferClient.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                checkNotHoldsLock(this);
                clientDeleteResponseCalledTimes.add(System.currentTimeMillis());
                while (clientDeleteResponseCalledTimes.size() > 5) {
                    clientDeleteResponseCalledTimes.poll();
                }

                log.error(t, "Request to delete %s failed", location);
                if (!(t instanceof PrestoException) && backoff.failure()) {
                    String message = format("Error closing remote buffer (%s - %s failures, failure duration %s, total failed request time %s)",
                            location,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    t = new PrestoException(REMOTE_BUFFER_CLOSE_FAILED, message, t);
                }
                handleFailure(t, resultFuture);
            }
        }, pageBufferClientCallbackExecutor);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Cannot execute this method while holding a lock");
    }

    private void handleFailure(Throwable t, ListenableFuture<?> expectedFuture)
    {
        // Can not delegate to other callback while holding a lock on this
        checkNotHoldsLock(this);

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof PrestoException) {
            clientCallback.clientFailed(PageBufferClient.this, t);
        }

        synchronized (PageBufferClient.this) {
            if (future == expectedFuture) {
                future = null;
            }
            lastUpdate = DateTime.now();
        }
        clientCallback.requestComplete(PageBufferClient.this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PageBufferClient that = (PageBufferClient) o;

        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (future != null) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return toStringHelper(this)
                .add("location", location)
                .addValue(state)
                .toString();
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(String taskInstanceId, long token, long nextToken, Iterable<SerializedPage> pages, boolean complete, boolean gracefulShutdown, long bufferedBytes)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, pages, complete, gracefulShutdown, bufferedBytes);
        }

        public static PagesResponse createEmptyPagesResponse(String taskInstanceId, long token, long nextToken, boolean complete, boolean gracefulShutdown, long bufferedBytes)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, ImmutableList.of(), complete, gracefulShutdown, bufferedBytes);
        }

        private final String taskInstanceId;
        private final long token;
        private final long nextToken;
        private final List<SerializedPage> pages;
        private final boolean clientComplete;
        private final boolean gracefulShutdown;
        private final long bufferedBytes;

        private PagesResponse(String taskInstanceId, long token, long nextToken, Iterable<SerializedPage> pages, boolean clientComplete, boolean gracefulShutdown, long bufferedBytes)
        {
            this.taskInstanceId = taskInstanceId;
            this.token = token;
            this.nextToken = nextToken;
            this.pages = ImmutableList.copyOf(pages);
            this.clientComplete = clientComplete;
            this.gracefulShutdown = gracefulShutdown;
            this.bufferedBytes = bufferedBytes;
        }

        public long getToken()
        {
            return token;
        }

        public long getNextToken()
        {
            return nextToken;
        }

        public List<SerializedPage> getPages()
        {
            return pages;
        }

        public boolean isClientComplete()
        {
            return clientComplete;
        }

        public boolean isServerGracefulShutdown()
        {
            return gracefulShutdown;
        }

        public long getBufferedBytes()
        {
            return bufferedBytes;
        }

        public String getTaskInstanceId()
        {
            return taskInstanceId;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("token", token)
                    .add("nextToken", nextToken)
                    .add("pagesSize", pages.size())
                    .add("clientComplete", clientComplete)
                    .add("gracefulShutdown", gracefulShutdown)
                    .add("bufferedBytes", bufferedBytes)
                    .toString();
        }
    }
}

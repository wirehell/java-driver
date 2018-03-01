/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.session.throttling;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A request throttler that limits the rate of requests per second. */
public class RateLimitingRequestThrottler implements RequestThrottler {

  private static final Logger LOG = LoggerFactory.getLogger(RateLimitingRequestThrottler.class);

  private final String logPrefix;
  private final NanoClock clock;
  private final int maxQueueSize;
  private final long drainIntervalNanos;

  private final EventExecutor scheduler;

  @VisibleForTesting final AtomicReference<State> stateRef;
  @VisibleForTesting final Deque<Throttled> queue = new ConcurrentLinkedDeque<>();

  @SuppressWarnings("unused")
  public RateLimitingRequestThrottler(DriverContext context) {
    this(context, System::nanoTime);
  }

  @VisibleForTesting
  RateLimitingRequestThrottler(DriverContext context, NanoClock clock) {
    this.logPrefix = context.sessionName();
    this.clock = clock;

    DriverConfigProfile config = context.config().getDefaultProfile();

    int maxRequestsPerSecond =
        config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND);
    this.stateRef = new AtomicReference<>(State.initial(maxRequestsPerSecond, clock.nanoTime()));
    this.maxQueueSize = config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE);
    Duration drainInterval =
        config.getDuration(DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL);
    this.drainIntervalNanos = drainInterval.toNanos();

    this.scheduler =
        ((InternalDriverContext) context).nettyOptions().adminEventExecutorGroup().next();

    LOG.debug(
        "[{}] Initializing with maxRequestsPerSecond = {}, maxQueueSize = {}, drainInterval = {}",
        logPrefix,
        maxRequestsPerSecond,
        maxQueueSize,
        drainInterval);
  }

  @Override
  public void register(Throttled request) {
    while (true) {
      State state = stateRef.get();
      long now = clock.nanoTime();
      if (state.closed) {
        LOG.trace("[{}] Rejecting request after shutdown", logPrefix);
        fail(request, "The session is shutting down");
        return;
      } else if (state.queueSize == 0 && state.canAcquire(now)) {
        if (stateRef.compareAndSet(state, state.acquire(now))) {
          LOG.trace("[{}] Starting newly registered request", logPrefix);
          request.onThrottleReady();
          return;
        }
      } else if (state.queueSize < maxQueueSize) {
        if (stateRef.compareAndSet(state, state.enqueue())) {
          LOG.trace("[{}] Enqueuing request", logPrefix);
          queue.add(request);
          if (state.queueSize == 0) { // we just started enqueuing
            scheduler.schedule(this::drain, drainIntervalNanos, TimeUnit.NANOSECONDS);
          }
          return;
        }
      } else {
        LOG.trace("[{}] Rejecting request because of full queue", logPrefix);
        fail(
            request,
            String.format(
                "The session has reached its maximum capacity "
                    + "(requests/s: %d, queue size: %d)",
                state.maxRequestsPerSecond, maxQueueSize));
        return;
      }
    }
  }

  // Runs periodically when the queue is not empty. It tries to dequeue as much as possible while
  // staying under the target rate. If it does not completely drain the queue, it reschedules
  // itself.
  private void drain() {
    assert scheduler.inEventLoop();
    while (true) {
      State state = stateRef.get();
      if (state.closed || state.queueSize == 0) {
        return;
      }

      long now = clock.nanoTime();
      int toDequeue = state.canAcquire(now, state.queueSize);
      if (toDequeue == 0) {
        LOG.trace("[{}] No capacity to dequeue, rescheduling drain task", logPrefix);
        scheduler.schedule(this::drain, drainIntervalNanos, TimeUnit.NANOSECONDS);
        return;
      }

      if (stateRef.compareAndSet(state, state.acquireAndDequeue(now, toDequeue))) {
        LOG.trace(
            "[{}] Draining queue ({} out of {} elements can be started now)",
            logPrefix,
            toDequeue,
            state.queueSize);
        int stillInQueue = state.queueSize - toDequeue;
        Throttled request;
        while (toDequeue > 0 && (request = queue.poll()) != null) {
          toDequeue -= 1;
          LOG.trace("[{}] Starting dequeued request", logPrefix);
          request.onThrottleReady();
        }
        if (stillInQueue > 0) {
          LOG.trace(
              "[{}] {} elements still in queue, rescheduling drain task", logPrefix, stillInQueue);
          scheduler.schedule(this::drain, drainIntervalNanos, TimeUnit.NANOSECONDS);
        }
        return;
      }
    }
  }

  @Override
  public void signalSuccess(Throttled request) {
    // nothing to do
  }

  @Override
  public void signalError(Throttled request, Throwable error) {
    // nothing to do
  }

  @Override
  public void signalTimeout(Throttled request) {
    if (queue.remove(request)) { // The request timed out before it was active
      while (true) {
        State state = stateRef.get();
        if (state.closed) {
          // Put it back because close() expects state.queueSize to match the queue exactly
          queue.add(request);
          return;
        } else if (stateRef.compareAndSet(state, state.dequeue())) {
          LOG.trace("[{}] Removing timed out request from the queue", logPrefix);
          return;
        }
      }
    }
  }

  @Override
  public void close() {
    RunOrSchedule.on(scheduler, this::confinedClose); // avoid races with drain()
  }

  private void confinedClose() {
    assert scheduler.inEventLoop();

    State state = stateRef.updateAndGet(State::close);
    // We can race with register() or signalTimeout() and have another thread enqueue/dequeue while
    // we're draining the queue. So wait for the exact number of elements at the time we closed
    // (any enqueue()/dequeue() call on the state happened-before marking it as closed).
    // This might spin-wait, but only for a few elements.
    int remaining = state.queueSize;
    while (remaining > 0) {
      Throttled request = queue.poll();
      if (request != null) {
        fail(request, "The session is shutting down");
        remaining -= 1;
        LOG.trace(
            "[{}] Rejecting queued request after shutdown, remaining = {}", logPrefix, remaining);
        if (remaining == 0) {
          return;
        }
      }
    }
  }

  private static void fail(Throttled request, String message) {
    request.onThrottleFailure(new RequestThrottlingException(message));
  }

  @VisibleForTesting
  static class State {

    static State initial(int requestsPerSecond, long currentTimeNanos) {
      return new State(requestsPerSecond, currentTimeNanos, requestsPerSecond, 0, false);
    }

    // Our lock-free algorithm borrows concepts from Guava's RateLimiter: we create a new permit at
    // every (1/maxRequestsPerSecond) interval; in addition, if the limiter was idle for longer, we
    // allow unused permits to accumulate up to a maximum (in our case, hard-coded to 1 second's
    // worth of permits). Notable differences:
    // - we only support "bursty" behavior: stored permits are distributed immediately, without any
    //   wait time.
    // - if a client requests N permits but only M < N are available, then it is assigned M and has
    //   to reschedule for the (N-M) remaining ones (in practice, only the draining task does that).
    final int maxRequestsPerSecond;
    final long lastUpdateNanos;
    final int storedPermits;

    final int queueSize;
    final boolean closed;

    private State(
        int maxRequestsPerSecond,
        long lastUpdateNanos,
        int storedPermits,
        int queueSize,
        boolean closed) {
      this.maxRequestsPerSecond = maxRequestsPerSecond;
      this.lastUpdateNanos = lastUpdateNanos;
      this.storedPermits = storedPermits;
      this.queueSize = queueSize;
      this.closed = closed;
    }

    boolean canAcquire(long currentTimeNanos) {
      return canAcquire(currentTimeNanos, 1) == 1;
    }

    /**
     * Precondition: a prior call to {@link #canAcquire(long)} on the same instance with the same
     * timestamp returned true.
     */
    State acquire(long currentTimeNanos) {
      int newStoredPermits = updateStoreAndAcquire(currentTimeNanos, 1);
      return new State(maxRequestsPerSecond, currentTimeNanos, newStoredPermits, queueSize, false);
    }

    /**
     * @return if {@code requestedPermits} is too much, the maximum number that keeps us under the
     *     threshold.
     */
    int canAcquire(long currentTimeNanos, int requestedPermits) {
      assert !closed;
      int newStoredPermits = updateStore(currentTimeNanos);
      return Math.min(requestedPermits, newStoredPermits);
    }

    /**
     * Precondition: a prior call to {@link #canAcquire(long, int)} on the same instance with the
     * same timestamp returned at least {@code amount}.
     */
    State acquireAndDequeue(long currentTimeNanos, int amount) {
      int newStoredPermits = updateStoreAndAcquire(currentTimeNanos, amount);
      return new State(
          maxRequestsPerSecond, currentTimeNanos, newStoredPermits, queueSize - amount, false);
    }

    private int updateStore(long currentTimeNanos) {
      long elapsedNanos = currentTimeNanos - lastUpdateNanos;
      if (elapsedNanos <= 0) {
        return storedPermits;
      } else if (elapsedNanos >= 1_000_000_000) {
        // created more than the max, so whatever was stored, the sum will be capped to the max
        return maxRequestsPerSecond;
      } else {
        int createdPermits = (int) (elapsedNanos * maxRequestsPerSecond / 1_000_000_000);
        return Math.min(storedPermits + createdPermits, maxRequestsPerSecond);
      }
    }

    private int updateStoreAndAcquire(long currentTimeNanos, int releasedPermits) {
      assert !closed;
      int newStoredPermits = updateStore(currentTimeNanos);
      if (newStoredPermits < releasedPermits) {
        throw new IllegalStateException("Should have checked that canAcquire > " + releasedPermits);
      }
      return newStoredPermits - releasedPermits;
    }

    State enqueue() {
      assert !closed;
      return new State(maxRequestsPerSecond, lastUpdateNanos, storedPermits, queueSize + 1, false);
    }

    State dequeue() {
      assert !closed && queueSize > 1;
      return new State(maxRequestsPerSecond, lastUpdateNanos, storedPermits, queueSize - 1, false);
    }

    State close() {
      assert !closed;
      return new State(maxRequestsPerSecond, lastUpdateNanos, storedPermits, queueSize, true);
    }
  }
}

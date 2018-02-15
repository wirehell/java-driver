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
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A request throttler that limits the number of concurrent requests. */
public class ConcurrencyLimitingRequestThrottler implements RequestThrottler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConcurrencyLimitingRequestThrottler.class);

  private final String logPrefix;
  private final int maxConcurrentRequests;
  private final int maxQueueSize;

  private final AtomicReference<State> stateRef = new AtomicReference<>(State.INITIAL);
  private final Deque<Throttled> queue = new ConcurrentLinkedDeque<>();

  public ConcurrencyLimitingRequestThrottler(DriverContext context) {
    this.logPrefix = context.sessionName();
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.maxConcurrentRequests =
        config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS);
    this.maxQueueSize = config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE);
    LOG.debug(
        "[{}] Initializing with maxConcurrentRequests = {}, maxQueueSize = {}",
        logPrefix,
        maxConcurrentRequests,
        maxQueueSize);
  }

  @Override
  public void register(Throttled request) {
    while (true) {
      State state = stateRef.get();
      if (state.closed) {
        LOG.trace("[{}] Rejecting request after shutdown", logPrefix);
        fail(request, "The session is shutting down");
        return;
      } else if (state.queueSize == 0 && state.concurrentRequests < maxConcurrentRequests) {
        // We have capacity for one more concurrent request
        if (stateRef.compareAndSet(state, state.acquirePermit())) {
          LOG.trace("[{}] Starting newly registered request", logPrefix);
          request.onThrottleReady();
          return;
        }
      } else if (state.queueSize < maxQueueSize) {
        // We don't have capacity but there's some room left in the queue
        if (stateRef.compareAndSet(state, state.enqueue())) {
          LOG.trace("[{}] Enqueuing request", logPrefix);
          queue.add(request);
          return;
        }
      } else {
        LOG.trace("[{}] Rejecting request because of full queue", logPrefix);
        fail(
            request,
            String.format(
                "The session has reached its maximum capacity "
                    + "(concurrent requests: %d, queue size: %d)",
                maxConcurrentRequests, maxQueueSize));
        return;
      }
    }
  }

  @Override
  public void signalSuccess(Throttled request) {
    onRequestDone();
  }

  @Override
  public boolean signalError(Throttled request, Throwable error) {
    onRequestDone();
    return true;
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
    } else {
      onRequestDone();
    }
  }

  private void onRequestDone() {
    // An active request just finished so we have capacity for more
    while (true) {
      State state = stateRef.get();
      if (state.closed) {
        return;
      } else if (state.queueSize == 0) {
        // No other request was waiting, simply decrease the count
        if (stateRef.compareAndSet(state, state.releasePermit())) {
          return;
        }
      } else if (stateRef.compareAndSet(state, state.dequeue())) {
        // Another request was waiting, start it (don't change the count because we released a
        // permit but immediately re-acquired it).
        Throttled request = queue.poll();
        assert request != null;
        LOG.trace("[{}] Starting dequeued request", logPrefix);
        request.onThrottleReady();
        return;
      }
    }
  }

  @Override
  public void close() {
    State state = stateRef.updateAndGet(State::close);
    // We can race with register() or onRequestDone() and have another thread enqueue/dequeue while
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

  private static class State {

    static final State INITIAL = new State(0, 0, false);

    final int concurrentRequests;
    final int queueSize;
    final boolean closed;

    State(int concurrentRequests, int queueSize, boolean closed) {
      this.concurrentRequests = concurrentRequests;
      this.queueSize = queueSize;
      this.closed = closed;
    }

    State acquirePermit() {
      assert !closed && queueSize == 0;
      return new State(concurrentRequests + 1, queueSize, false);
    }

    State releasePermit() {
      assert !closed && queueSize == 0 && concurrentRequests > 0;
      return new State(concurrentRequests - 1, 0, false);
    }

    State enqueue() {
      assert !closed;
      return new State(concurrentRequests, queueSize + 1, false);
    }

    State dequeue() {
      assert !closed && queueSize > 0;
      return new State(concurrentRequests, queueSize - 1, false);
    }

    State close() {
      assert !closed;
      return new State(concurrentRequests, queueSize, true);
    }
  }
}

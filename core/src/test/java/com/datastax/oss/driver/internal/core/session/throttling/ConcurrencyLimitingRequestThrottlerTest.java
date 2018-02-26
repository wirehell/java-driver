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

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler.State;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConcurrencyLimitingRequestThrottlerTest {

  @Mock private DriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultProfile;

  private ConcurrencyLimitingRequestThrottler throttler;

  @Before
  public void setup() {
    Mockito.when(context.config()).thenReturn(config);
    Mockito.when(config.getDefaultProfile()).thenReturn(defaultProfile);

    Mockito.when(
            defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS))
        .thenReturn(5);
    Mockito.when(defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE))
        .thenReturn(10);

    throttler = new ConcurrencyLimitingRequestThrottler(context);
  }

  @Test
  public void should_start_immediately_when_under_capacity() {
    // Given
    MockThrottled request = new MockThrottled();

    // When
    throttler.register(request);

    // Then
    assertThat(request.started).isSuccess();
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(1);
    assertThat(state.queueSize).isEqualTo(0);
  }

  @Test
  public void should_allow_new_request_when_active_one_succeeds() {
    should_allow_new_request_when_active_one_completes(throttler::signalSuccess);
  }

  @Test
  public void should_allow_new_request_when_active_one_fails() {
    should_allow_new_request_when_active_one_completes(
        request -> throttler.signalError(request, new RuntimeException("mock error")));
  }

  @Test
  public void should_allow_new_request_when_active_one_times_out() {
    should_allow_new_request_when_active_one_completes(throttler::signalTimeout);
  }

  private void should_allow_new_request_when_active_one_completes(
      Consumer<Throttled> completeCallback) {
    // Given
    MockThrottled first = new MockThrottled();
    throttler.register(first);
    assertThat(first.started).isSuccess();
    for (int i = 0; i < 4; i++) { // fill to capacity
      throttler.register(new MockThrottled());
    }
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(0);

    // When
    completeCallback.accept(first);
    state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(4);
    assertThat(state.queueSize).isEqualTo(0);
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThat(incoming.started).isSuccess();
    state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(0);
  }

  @Test
  public void should_enqueue_when_over_capacity() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(0);

    // When
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThat(incoming.started).isNotDone();
    state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(1);
    assertThat(throttler.queue).containsExactly(incoming);
  }

  @Test
  public void should_dequeue_when_active_succeeds() {
    should_dequeue_when_active_completes(throttler::signalSuccess);
  }

  @Test
  public void should_dequeue_when_active_fails() {
    should_dequeue_when_active_completes(
        request -> throttler.signalError(request, new RuntimeException("mock error")));
  }

  @Test
  public void should_dequeue_when_active_times_out() {
    should_dequeue_when_active_completes(throttler::signalTimeout);
  }

  private void should_dequeue_when_active_completes(Consumer<Throttled> completeCallback) {
    // Given
    MockThrottled first = new MockThrottled();
    throttler.register(first);
    assertThat(first.started).isSuccess();
    for (int i = 0; i < 4; i++) {
      throttler.register(new MockThrottled());
    }

    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);
    assertThat(incoming.started).isNotDone();

    // When
    completeCallback.accept(first);

    // Then
    assertThat(incoming.started).isSuccess();
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(0);
  }

  @Test
  public void should_reject_when_queue_is_full() {
    // Given
    for (int i = 0; i < 15; i++) {
      throttler.register(new MockThrottled());
    }
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(10);

    // When
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThat(incoming.started)
        .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
  }

  @Test
  public void should_remove_timed_out_request_from_queue() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    MockThrottled queued1 = new MockThrottled();
    throttler.register(queued1);
    MockThrottled queued2 = new MockThrottled();
    throttler.register(queued2);

    // When
    throttler.signalTimeout(queued1);

    // Then
    assertThat(queued2.started).isNotDone();
    State state = throttler.stateRef.get();
    assertThat(state.concurrentRequests).isEqualTo(5);
    assertThat(state.queueSize).isEqualTo(1);
  }

  @Test
  public void should_reject_enqueued_when_closing() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    List<MockThrottled> enqueued = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      MockThrottled request = new MockThrottled();
      throttler.register(request);
      assertThat(request.started).isNotDone();
      enqueued.add(request);
    }

    // When
    throttler.close();

    // Then
    for (MockThrottled request : enqueued) {
      assertThat(request.started)
          .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
    }

    // When
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThat(request.started)
        .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
  }
}

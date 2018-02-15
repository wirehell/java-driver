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

import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler.State;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class RateLimitingRequestThrottlerStateTest {

  private static final long ONE_HUNDRED_MS =
      TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS);

  @Test
  public void should_start_with_one_seconds_worth_of_permits() {
    State state = State.initial(10, 0);
    assertThat(state.canAcquire(0, 100)).isEqualTo(10);
  }

  @Test
  public void should_create_new_permits_at_requested_rate() {
    long now = 0;
    State state = State.initial(10, now);

    // Consume the 10 initial permits
    for (int i = 0; i < 10; i++) {
      state = state.acquire(now);
    }

    // Should create exactly one permit every 1/10 second after that
    for (int i = 0; i < 10; i++) {
      now += ONE_HUNDRED_MS;
      assertThat(state.canAcquire(now)).isTrue();
      state = state.acquire(now);
      assertThat(state.canAcquire(now)).isFalse();
    }
  }

  @Test
  public void should_store_new_permits_up_to_threshold() {
    long now = 0;
    State state = State.initial(10, now);

    // Consume half of the initial permits
    for (int i = 0; i < 5; i++) {
      state = state.acquire(now);
    }

    // Should create exactly one additional permit every 1/10 second after that
    for (int i = 1; i <= 5; i++) {
      now += ONE_HUNDRED_MS;
      assertThat(state.canAcquire(now, 100)).isEqualTo(5 + i);
    }

    // Should max out to 10 permits after that
    for (int i = 1; i <= 5; i++) {
      now += ONE_HUNDRED_MS;
      assertThat(state.canAcquire(now, 100)).isEqualTo(10);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void should_fail_to_acquire_more_than_available() {
    State state = State.initial(10, 0);
    assertThat(state.canAcquire(0, 100)).isEqualTo(10);
    state.acquireAndDequeue(0, 11);
  }
}

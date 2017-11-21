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
package com.datastax.oss.driver.api.core.time;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.time.Clock;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A timestamp generator that guarantees monotonicity, and logs warnings when timestamps drift in
 * the future.
 */
abstract class MonotonicTimestampGenerator implements TimestampGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(MonotonicTimestampGenerator.class);

  private final Clock clock;
  private final long warningThresholdMicros;
  private final long warningIntervalMillis;
  private final AtomicLong lastDriftWarning = new AtomicLong(Long.MIN_VALUE);

  protected MonotonicTimestampGenerator(DriverContext context, DriverOption configRoot) {
    this(buildClock(context, configRoot), context, configRoot);
  }

  @VisibleForTesting
  protected MonotonicTimestampGenerator(
      Clock clock, DriverContext context, DriverOption configRoot) {
    this.clock = clock;

    DriverOption warningThresholdOption =
        configRoot.concat(CoreDriverOption.RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD);
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.warningThresholdMicros =
        (config.isDefined(warningThresholdOption))
            ? config.getDuration(warningThresholdOption).toNanos() / 1000
            : 0;

    if (this.warningThresholdMicros == 0) {
      this.warningIntervalMillis = 0;
    } else {
      DriverOption warningIntervalOption =
          configRoot.concat(CoreDriverOption.RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL);
      this.warningIntervalMillis = config.getDuration(warningIntervalOption).toMillis();
    }
  }

  /**
   * Compute the next timestamp, given the current clock tick and the last timestamp returned.
   *
   * <p>If timestamps have to drift ahead of the current clock tick to guarantee monotonicity, a
   * warning will be logged according to the rules defined in the configuration.
   */
  protected long computeNext(long last) {
    long currentTick = clock.currentTimeMicros();
    if (last >= currentTick) {
      maybeLog(currentTick, last);
      return last + 1;
    }
    return currentTick;
  }

  private void maybeLog(long currentTick, long last) {
    if (warningThresholdMicros != 0
        && LOG.isWarnEnabled()
        && last > currentTick + warningThresholdMicros) {
      long now = System.currentTimeMillis();
      long lastWarning = lastDriftWarning.get();
      if (now > lastWarning + warningIntervalMillis
          && lastDriftWarning.compareAndSet(lastWarning, now)) {
        LOG.warn(
            "Clock skew detected: current tick ({}) was {} microseconds behind the last generated timestamp ({}), "
                + "returned timestamps will be artificially incremented to guarantee monotonicity.",
            currentTick,
            last - currentTick,
            last);
      }
    }
  }

  private static Clock buildClock(DriverContext context, DriverOption configRoot) {
    DriverOption forceJavaClockOption =
        configRoot.concat(CoreDriverOption.RELATIVE_TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK);
    DriverConfigProfile config = context.config().getDefaultProfile();
    boolean forceJavaClock =
        config.isDefined(forceJavaClockOption) && config.getBoolean(forceJavaClockOption);
    return Clock.getInstance(forceJavaClock);
  }
}

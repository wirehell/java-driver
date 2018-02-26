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
package com.datastax.oss.driver.internal.core.adminrequest;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.Throttled;
import com.datastax.oss.protocol.internal.Message;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class ThrottledAdminRequestHandler extends AdminRequestHandler implements Throttled {

  private final RequestThrottler throttler;

  public ThrottledAdminRequestHandler(
      DriverChannel channel,
      Message message,
      Map<String, ByteBuffer> customPayload,
      Duration timeout,
      RequestThrottler throttler,
      String logPrefix,
      String debugString) {
    super(channel, message, customPayload, timeout, logPrefix, debugString);
    this.throttler = throttler;
  }

  @Override
  public CompletionStage<AdminResult> start() {
    // Don't write request yet, wait for green light from throttler
    throttler.register(this);
    return result;
  }

  @Override
  public void onThrottleReady() {
    super.start();
  }

  @Override
  public void onThrottleFailure(RequestThrottlingException error) {
    setFinalError(error);
  }

  @Override
  protected boolean setFinalResult(AdminResult result) {
    boolean wasSet = super.setFinalResult(result);
    if (wasSet) {
      throttler.signalSuccess(this);
    }
    return wasSet;
  }

  @Override
  protected boolean setFinalError(Throwable error) {
    boolean wasSet = super.setFinalError(error);
    if (wasSet) {
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
    return wasSet;
  }
}

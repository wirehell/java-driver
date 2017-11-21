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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import java.net.SocketAddress;

/**
 * Thrown when a heartbeat query fails.
 *
 * <p>Heartbeat queries are sent automatically on idle connections, to ensure that they are still
 * alive. If a heartbeat query fails, the connection is closed, and all pending queries are aborted.
 * The exception will be passed to {@link RetryPolicy#onRequestAborted(Request, Throwable, int)},
 * which decides what to do next (the default policy retries the query on the next node).
 */
public class HeartbeatException extends DriverException {

  private final SocketAddress address;

  public HeartbeatException(SocketAddress address, String message, Throwable cause) {
    super(message, cause, true);
    this.address = address;
  }

  /** The address of the node that encountered the error. */
  public SocketAddress getAddress() {
    return address;
  }

  @Override
  public DriverException copy() {
    return new HeartbeatException(address, getMessage(), getCause());
  }
}

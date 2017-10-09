/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.guava;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.guava.DefaultGuavaSession;
import com.google.common.util.concurrent.ListenableFuture;

public interface GuavaSession extends Session {
  ListenableFuture<AsyncResultSet> executeAsync(Statement<?> statement);

  ListenableFuture<AsyncResultSet> executeAsync(String statement);

  ListenableFuture<PreparedStatement> prepareAsync(SimpleStatement statement);

  ListenableFuture<PreparedStatement> prepareAsync(String statement);

  static GuavaSession wrap(Session session) {
    return new DefaultGuavaSession(session);
  }
}

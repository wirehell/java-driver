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
package com.datastax.oss.driver.internal.guava;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.guava.GuavaSession;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletionStage;

public class DefaultGuavaSession implements GuavaSession {

  static final GenericType<ListenableFuture<AsyncResultSet>> ASYNC =
      new GenericType<ListenableFuture<AsyncResultSet>>() {};

  static final GenericType<ListenableFuture<PreparedStatement>> ASYNC_PREPARED =
      new GenericType<ListenableFuture<PreparedStatement>>() {};

  private final Session delegate;

  public DefaultGuavaSession(Session delegate) {
    this.delegate = delegate;
  }

  @Override
  public ListenableFuture<AsyncResultSet> executeAsync(Statement<?> statement) {
    return this.execute(statement, ASYNC);
  }

  @Override
  public ListenableFuture<AsyncResultSet> executeAsync(String statement) {
    return this.executeAsync(SimpleStatement.newInstance(statement));
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(SimpleStatement statement) {
    return this.execute(statement, ASYNC_PREPARED);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String statement) {
    return this.prepareAsync(SimpleStatement.newInstance(statement));
  }

  @Override
  public CompletionStage<Void> closeFuture() {
    return delegate.closeFuture();
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    return delegate.closeAsync();
  }

  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return delegate.forceCloseAsync();
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType) {
    return delegate.execute(request, resultType);
  }
}

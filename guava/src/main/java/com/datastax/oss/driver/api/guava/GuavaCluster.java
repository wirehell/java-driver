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

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.internal.core.DefaultCluster;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.guava.ListenableFutureUtils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class GuavaCluster implements Cluster {

  private final Cluster delegate;

  public static ListenableFuture<GuavaCluster> init(
      InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
    return ListenableFutureUtils.toListenableFuture(
        DefaultCluster.init(context, contactPoints).thenApply(GuavaCluster::new));
  }

  private GuavaCluster(Cluster delegate) {
    this.delegate = delegate;
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
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Metadata getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public DriverContext getContext() {
    return delegate.getContext();
  }

  @Override
  public CompletionStage<CqlSession> connectAsync(CqlIdentifier keyspace) {
    return delegate.connectAsync(keyspace);
  }

  public ListenableFuture<GuavaSession> connectAsyncG(CqlIdentifier keyspace) {
    return ListenableFutureUtils.toListenableFuture(
        delegate.connectAsync(keyspace).thenApply(GuavaSession::wrap));
  }

  public GuavaSession connectG() {
    return connectG(null);
  }

  public GuavaSession connectG(CqlIdentifier keyspace) {
    try {
      return Uninterruptibles.getUninterruptibly(connectAsyncG(keyspace));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static GuavaClusterBuilder builder() {
    return new GuavaClusterBuilder();
  }
}

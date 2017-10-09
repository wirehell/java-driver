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
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.internal.core.ClusterWrapper;
import com.datastax.oss.driver.internal.core.DefaultCluster;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.guava.ListenableFutureUtils;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.util.Set;

public class GuavaCluster extends ClusterWrapper<CqlSession, GuavaSession> {

  public static ListenableFuture<GuavaCluster> init(
      InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
    return ListenableFutureUtils.toListenableFuture(
        DefaultCluster.init(context, contactPoints).thenApply(GuavaCluster::new));
  }

  private GuavaCluster(Cluster<CqlSession> delegate) {
    super(delegate);
  }

  @Override
  protected GuavaSession wrap(CqlSession session) {
    return GuavaSession.wrap(session);
  }

  public static GuavaClusterBuilder builder() {
    return new GuavaClusterBuilder();
  }
}

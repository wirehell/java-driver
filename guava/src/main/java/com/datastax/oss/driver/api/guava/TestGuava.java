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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class TestGuava {

  public static void main(String args[]) throws InterruptedException {
    try (GuavaCluster cluster =
        GuavaCluster.builder().addContactPoint(new InetSocketAddress("localhost", 9042)).build()) {
      GuavaSession session = cluster.connect();

      CountDownLatch latch = new CountDownLatch(1);
      ListenableFuture<AsyncResultSet> result =
          Futures.transformAsync(
              session.prepareAsync("select host_id from system.local where key = ?"),
              prepared -> session.executeAsync(prepared.bind("local")));

      Futures.addCallback(
          result,
          new FutureCallback<AsyncResultSet>() {
            @Override
            public void onSuccess(AsyncResultSet result) {
              System.out.println(result.currentPage().iterator().next().getUuid("host_id"));
              latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
              t.printStackTrace();
              latch.countDown();
            }
          });

      latch.await();
    }
  }
}

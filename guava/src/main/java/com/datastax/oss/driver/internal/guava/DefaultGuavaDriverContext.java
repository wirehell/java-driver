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

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.DefaultPreparedStatement;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.google.common.collect.MapMaker;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class DefaultGuavaDriverContext extends DefaultDriverContext {

  public DefaultGuavaDriverContext(DriverConfigLoader configLoader, List<TypeCodec<?>> typeCodecs) {
    super(configLoader, typeCodecs);
  }

  @Override
  public RequestProcessorRegistry requestProcessorRegistry() {
    ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache =
        new MapMaker().weakValues().makeMap();

    CqlRequestAsyncProcessor cqlRequestAsyncProcessor = new CqlRequestAsyncProcessor();
    CqlPrepareAsyncProcessor cqlPrepareAsyncProcessor =
        new CqlPrepareAsyncProcessor(preparedStatementsCache);

    return new RequestProcessorRegistry(
        clusterName(),
        new CqlRequestSyncProcessor(),
        new CqlPrepareSyncProcessor(preparedStatementsCache),
        new GuavaRequestAsyncProcessor<>(
            cqlRequestAsyncProcessor, Statement.class, DefaultGuavaSession.ASYNC),
        new GuavaRequestAsyncProcessor<>(
            cqlPrepareAsyncProcessor, PrepareRequest.class, DefaultGuavaSession.ASYNC_PREPARED));
  }
}

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
package com.datastax.oss.driver.api.core.ssl;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Default SSL implementation.
 *
 * <p>To activate this class, an {@code ssl-engine-factory} section must be included in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   ssl-engine-factory {
 *     class = com.datastax.driver.api.core.ssl.DefaultSslEngineFactory
 *     cipher-suites = [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]
 *   }
 * }
 * </pre>
 *
 * See the {@code reference.conf} file included with the driver for more information.
 */
public class DefaultSslEngineFactory implements SslEngineFactory {

  private final SSLContext sslContext;
  private final String[] cipherSuites;

  /** Builds a new instance from the driver configuration. */
  public DefaultSslEngineFactory(DriverContext driverContext, DriverOption configRoot) {
    try {
      this.sslContext = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Cannot initialize SSL Context", e);
    }
    DriverConfigProfile config = driverContext.config().getDefaultProfile();
    DriverOption cipherSuiteOption =
        configRoot.concat(CoreDriverOption.RELATIVE_DEFAULT_SSL_CIPHER_SUITES);
    if (config.isDefined(cipherSuiteOption)) {
      List<String> list = config.getStringList(cipherSuiteOption);
      String tmp[] = new String[list.size()];
      this.cipherSuites = list.toArray(tmp);
    } else {
      this.cipherSuites = null;
    }
  }

  @Override
  public SSLEngine newSslEngine(SocketAddress remoteEndpoint) {
    SSLEngine engine;
    if (remoteEndpoint instanceof InetSocketAddress) {
      InetSocketAddress address = (InetSocketAddress) remoteEndpoint;
      engine = sslContext.createSSLEngine(address.getHostName(), address.getPort());
    } else {
      engine = sslContext.createSSLEngine();
    }
    engine.setUseClientMode(true);
    if (cipherSuites != null) {
      engine.setEnabledCipherSuites(cipherSuites);
    }
    return engine;
  }
}

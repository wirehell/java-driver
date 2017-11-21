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
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import java.nio.ByteBuffer;

/**
 * A specialized integer codec that knows how to deal with primitive types.
 *
 * <p>If the codec registry returns an instance of this type, the driver's integer getters will use
 * it to avoid boxing.
 */
public interface PrimitiveIntCodec extends TypeCodec<Integer> {

  ByteBuffer encodePrimitive(int value, ProtocolVersion protocolVersion);

  int decodePrimitive(ByteBuffer value, ProtocolVersion protocolVersion);

  @Override
  default ByteBuffer encode(Integer value, ProtocolVersion protocolVersion) {
    return (value == null) ? null : encodePrimitive(value, protocolVersion);
  }

  @Override
  default Integer decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return (bytes == null || bytes.remaining() == 0)
        ? null
        : decodePrimitive(bytes, protocolVersion);
  }
}

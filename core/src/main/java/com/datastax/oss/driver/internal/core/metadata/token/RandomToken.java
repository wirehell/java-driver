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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.google.common.base.Preconditions;
import java.math.BigInteger;

/** A token generated by {@code RandomPartitioner}. */
public class RandomToken implements Token {

  private final BigInteger value;

  public RandomToken(BigInteger value) {
    this.value = value;
  }

  public BigInteger getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof RandomToken) {
      RandomToken that = (RandomToken) other;
      return this.value.equals(that.value);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public int compareTo(Token other) {
    Preconditions.checkArgument(
        other instanceof RandomToken, "Can only compare tokens of the same type");
    RandomToken that = (RandomToken) other;
    return this.value.compareTo(that.getValue());
  }

  @Override
  public String toString() {
    return "RandomToken(" + value + ")";
  }
}

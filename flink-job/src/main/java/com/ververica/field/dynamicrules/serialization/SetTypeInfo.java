/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.serialization;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Set;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

/**
 * Special {@code TypeInformation} used for sets.
 *
 * @param <E> The type of the entries in the set.
 */
@PublicEvolving
public class SetTypeInfo<E> extends TypeInformation<Set<E>> {

  /* The type information for the entries in the set */
  private final TypeInformation<E> entryTypeInfo;

  public SetTypeInfo(TypeInformation<E> entryTypeInfo) {
    this.entryTypeInfo =
        Preconditions.checkNotNull(entryTypeInfo, "The entry type information cannot be null.");
  }

  public SetTypeInfo(Class<E> entryClass) {
    this.entryTypeInfo = of(checkNotNull(entryClass, "The entry class cannot be null."));
  }

  // ------------------------------------------------------------------------
  //  TypeInformation implementation
  // ------------------------------------------------------------------------

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<Set<E>> getTypeClass() {
    return (Class<Set<E>>) (Class<?>) Set.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<Set<E>> createSerializer(ExecutionConfig config) {
    TypeSerializer<E> entryTypeSerializer = entryTypeInfo.createSerializer(config);

    return new SetSerializer<>(entryTypeSerializer);
  }

  @Override
  public String toString() {
    return "Set<" + entryTypeInfo + ">";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof SetTypeInfo) {
      @SuppressWarnings("unchecked")
      SetTypeInfo<E> other = (SetTypeInfo<E>) obj;

      return (other.canEqual(this) && entryTypeInfo.equals(other.entryTypeInfo));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return entryTypeInfo.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return (obj != null && obj.getClass() == getClass());
  }
}

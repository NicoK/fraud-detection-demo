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

import java.util.Set;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Snapshot class for the {@link SetSerializer}. */
public class SetSerializerSnapshot<E>
    extends CompositeTypeSerializerSnapshot<Set<E>, SetSerializer<E>> {

  private static final int CURRENT_VERSION = 1;

  /** Constructor for read instantiation. */
  @SuppressWarnings("unused")
  public SetSerializerSnapshot() {
    super(SetSerializer.class);
  }

  /** Constructor to create the snapshot for writing. */
  public SetSerializerSnapshot(SetSerializer<E> setSerializer) {
    super(setSerializer);
  }

  @Override
  public int getCurrentOuterSnapshotVersion() {
    return CURRENT_VERSION;
  }

  @Override
  protected SetSerializer<E> createOuterSerializerWithNestedSerializers(
      TypeSerializer<?>[] nestedSerializers) {
    @SuppressWarnings("unchecked")
    TypeSerializer<E> entrySerializer = (TypeSerializer<E>) nestedSerializers[0];

    return new SetSerializer<>(entrySerializer);
  }

  @Override
  protected TypeSerializer<?>[] getNestedSerializers(SetSerializer<E> outerSerializer) {
    return new TypeSerializer<?>[] {outerSerializer.getEntrySerializer()};
  }
}

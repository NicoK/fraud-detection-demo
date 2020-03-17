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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

/**
 * A serializer for {@link Set}. The serializer relies on an entry serializer for the serialization
 * of the sets's entries.
 *
 * <p>The serialization format for the set is as follows: four bytes for the length of the set,
 * followed by the serialized representation of each entry.
 *
 * @param <E> The type of the entries in the set.
 */
public final class SetSerializer<E> extends TypeSerializer<Set<E>> {

  private static final long serialVersionUID = -6885593032367050078L;

  /** The serializer for the entries in the set */
  private final TypeSerializer<E> entrySerializer;

  /**
   * Creates a set serializer that uses the given serializer to serialize the entries in the set.
   *
   * @param entrySerializer The serializer for the entries in the set
   */
  public SetSerializer(TypeSerializer<E> entrySerializer) {
    this.entrySerializer =
        Preconditions.checkNotNull(entrySerializer, "The entry serializer cannot be null.");
  }

  // ------------------------------------------------------------------------
  //  SetSerializer specific properties
  // ------------------------------------------------------------------------

  public TypeSerializer<E> getEntrySerializer() {
    return entrySerializer;
  }

  // ------------------------------------------------------------------------
  //  Type Serializer implementation
  // ------------------------------------------------------------------------

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Set<E>> duplicate() {
    TypeSerializer<E> duplicateEntrySerializer = entrySerializer.duplicate();

    return (duplicateEntrySerializer == entrySerializer)
        ? this
        : new SetSerializer<>(duplicateEntrySerializer);
  }

  @Override
  public Set<E> createInstance() {
    return new HashSet<>();
  }

  @Override
  public Set<E> copy(Set<E> from) {
    Set<E> newSet = new HashSet<>(from.size());

    for (E entry : from) {
      newSet.add(entrySerializer.copy(entry));
    }

    return newSet;
  }

  @Override
  public Set<E> copy(Set<E> from, Set<E> reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1; // var length
  }

  @Override
  public void serialize(Set<E> set, DataOutputView target) throws IOException {
    final int size = set.size();
    target.writeInt(size);

    for (E entry : set) {
      entrySerializer.serialize(entry, target);
    }
  }

  @Override
  public Set<E> deserialize(DataInputView source) throws IOException {
    final int size = source.readInt();

    final Set<E> set = new HashSet<>(size);
    for (int i = 0; i < size; ++i) {
      set.add(entrySerializer.deserialize(source));
    }

    return set;
  }

  @Override
  public Set<E> deserialize(Set<E> reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    final int size = source.readInt();
    target.writeInt(size);

    for (int i = 0; i < size; ++i) {
      entrySerializer.copy(source, target);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || (obj != null
            && obj.getClass() == getClass()
            && entrySerializer.equals(((SetSerializer<?>) obj).getEntrySerializer()));
  }

  @Override
  public int hashCode() {
    return entrySerializer.hashCode();
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting
  // --------------------------------------------------------------------------------------------

  @Override
  public TypeSerializerSnapshot<Set<E>> snapshotConfiguration() {
    return new SetSerializerSnapshot<>(this);
  }
}

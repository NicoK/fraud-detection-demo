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

package com.ververica.field.dynamicrules.accumulators;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * An accumulator that computes the average value. Input can be {@code long} or {@code integer} and
 * the result is {@code long}.
 */
@Public
public class LongAverageAccumulator implements SimpleAccumulator<Long> {

  private static final long serialVersionUID = 1L;

  private long count;

  private long sum;

  @Override
  public void add(Long value) {
    this.count++;
    this.sum = Math.addExact(this.sum, value);
  }

  public void add(int value) {
    this.count++;
    this.sum = Math.addExact(this.sum, value);
  }

  @Override
  public Long getLocalValue() {
    if (this.count == 0L) {
      return 0L;
    }
    return new BigDecimal(this.sum).divide(new BigDecimal(count), RoundingMode.HALF_UP).longValue();
  }

  @Override
  public void resetLocal() {
    this.count = 0L;
    this.sum = 0L;
  }

  @Override
  public void merge(Accumulator<Long, Long> other) {
    if (other instanceof LongAverageAccumulator) {
      LongAverageAccumulator avg = (LongAverageAccumulator) other;
      this.count += avg.count;
      this.sum = Math.addExact(this.sum, avg.sum);
    } else {
      throw new IllegalArgumentException("The merged accumulator must be LongAverageAccumulator.");
    }
  }

  @Override
  public LongAverageAccumulator clone() {
    LongAverageAccumulator average = new LongAverageAccumulator();
    average.count = this.count;
    average.sum = this.sum;
    return average;
  }

  @Override
  public String toString() {
    return "LongAverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
  }
}

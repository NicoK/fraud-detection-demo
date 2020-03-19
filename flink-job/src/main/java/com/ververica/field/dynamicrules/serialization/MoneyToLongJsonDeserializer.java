/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.math.BigDecimal;

public class MoneyToLongJsonDeserializer extends JsonDeserializer<Long> {
  @Override
  public Long deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    //    return moneyToLong(jsonParser.getDecimalValue());
    return moneyToLong(jsonParser.readValueAs(BigDecimal.class));
  }

  public static long moneyToLong(BigDecimal money) {
    return money.scaleByPowerOfTen(2).longValueExact();
  }
}

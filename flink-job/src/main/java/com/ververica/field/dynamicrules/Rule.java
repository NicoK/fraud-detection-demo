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

package com.ververica.field.dynamicrules;

import static com.ververica.field.dynamicrules.serialization.LongToMoneyJsonSerializer.longToMoney;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.ververica.field.dynamicrules.serialization.LongToMoneyJsonSerializer;
import com.ververica.field.dynamicrules.serialization.MoneyToLongJsonDeserializer;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/** Rules representation. */
@EqualsAndHashCode
@Data
@TypeInfo(Rule.RuleTypeInfoFactory.class)
public class Rule {

  private Integer ruleId;
  private RuleState ruleState;
  private List<String> groupingKeyNames; // aggregation
  private String aggregateFieldName;
  private AggregatorFunctionType aggregatorFunctionType;
  private LimitOperatorType limitOperatorType;

  @JsonDeserialize(using = MoneyToLongJsonDeserializer.class)
  @JsonSerialize(using = LongToMoneyJsonSerializer.class)
  private long limit;

  private Integer windowMinutes;
  private ControlType controlType;

  public Long getWindowMillis() {
    return Time.minutes(this.windowMinutes).toMilliseconds();
  }

  /**
   * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
   *
   * @param comparisonValue value to be compared with the limit
   */
  public boolean apply(long comparisonValue) {
    switch (limitOperatorType) {
      case EQUAL:
        return comparisonValue == limit;
      case NOT_EQUAL:
        return comparisonValue != limit;
      case GREATER:
        return comparisonValue > limit;
      case LESS:
        return comparisonValue < limit;
      case LESS_EQUAL:
        return comparisonValue <= limit;
      case GREATER_EQUAL:
        return comparisonValue >= limit;
      default:
        throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
    }
  }

  public long getWindowStartFor(Long timestamp) {
    Long ruleWindowMillis = getWindowMillis();
    return (timestamp - ruleWindowMillis);
  }

  @Override
  public String toString() {
    return "Rule{"
        + "ruleId="
        + ruleId
        + ", ruleState="
        + ruleState
        + ", groupingKeyNames="
        + groupingKeyNames
        + ", aggregateFieldName='"
        + aggregateFieldName
        + '\''
        + ", aggregatorFunctionType="
        + aggregatorFunctionType
        + ", limitOperatorType="
        + limitOperatorType
        + ", limit="
        + longToMoney(limit)
        + ", windowMinutes="
        + windowMinutes
        + ", controlType="
        + controlType
        + '}';
  }

  public enum AggregatorFunctionType {
    SUM,
    AVG,
    MIN,
    MAX
  }

  public enum LimitOperatorType {
    EQUAL("="),
    NOT_EQUAL("!="),
    GREATER_EQUAL(">="),
    LESS_EQUAL("<="),
    GREATER(">"),
    LESS("<");

    String operator;

    LimitOperatorType(String operator) {
      this.operator = operator;
    }

    public static LimitOperatorType fromString(String text) {
      for (LimitOperatorType b : LimitOperatorType.values()) {
        if (b.operator.equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  public enum RuleState {
    ACTIVE,
    PAUSE,
    DELETE,
    CONTROL
  }

  public enum ControlType {
    CLEAR_STATE_ALL,
    DELETE_RULES_ALL,
    EXPORT_RULES_CURRENT
  }

  public static class RuleTypeInfoFactory extends TypeInfoFactory<Rule> {
    @Override
    public TypeInformation<Rule> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      return Types.POJO(
          Rule.class,
          new HashMap<String, TypeInformation<?>>() {
            {
              put("ruleId", Types.INT);
              put("ruleState", Types.ENUM(RuleState.class));
              put("groupingKeyNames", Types.LIST(Types.STRING));
              put("aggregateFieldName", Types.STRING);
              put("aggregatorFunctionType", Types.ENUM(AggregatorFunctionType.class));
              put("limitOperatorType", Types.ENUM(LimitOperatorType.class));
              put("limit", Types.LONG);
              put("windowMinutes", Types.INT);
              put("controlType", Types.ENUM(ControlType.class));
            }
          });
    }
  }
}

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

package com.ververica.field.dynamicrules.functions;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;
import static com.ververica.field.dynamicrules.serialization.LongToMoneyJsonSerializer.longToMoney;

import com.ververica.field.dynamicrules.Alert;
import com.ververica.field.dynamicrules.FieldsExtractor;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RuleHelper;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.serialization.SetTypeInfo;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class DynamicAlertFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, Rule, Alert<Transaction, BigDecimal>> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;

  private transient MapState<Long, Set<Transaction>> windowState;
  private Meter alertMeter;
  private transient Map<String, Field> transactionFields;

  private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
      new MapStateDescriptor<>("windowState", Types.LONG, new SetTypeInfo<>(Transaction.class));

  @Override
  public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);

    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    transactionFields = Transaction.getFieldMap();
  }

  @Override
  public void processElement(
      Keyed<Transaction, String, Integer> value,
      ReadOnlyContext ctx,
      Collector<Alert<Transaction, BigDecimal>> out)
      throws Exception {

    long currentEventTime = value.getWrapped().getEventTime();

    addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

    long ingestionTime = value.getWrapped().getIngestionTimestamp();
    ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

    Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());

    if (rule == null) {
      // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
      // updated and used in `DynamicKeyFunction`
      return;
    }

    if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
      Field aggregateField = transactionFields.get(rule.getAggregateFieldName());
      Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

      long cleanupTime = (currentEventTime / 1000) * 1000;
      ctx.timerService().registerEventTimeTimer(cleanupTime);

      SimpleAccumulator<Long> aggregator = RuleHelper.getAggregator(rule);
      for (Long stateEventTime : windowState.keys()) {
        if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
          aggregateValuesInState(stateEventTime, aggregator, rule, aggregateField);
        }
      }
      Long aggregateResult = aggregator.getLocalValue();
      boolean ruleResult = rule.apply(aggregateResult);

      log.trace(
          "Rule {} | {} : {} -> {}", rule.getRuleId(), value.getKey(), aggregateResult, ruleResult);

      if (ruleResult) {
        if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
          evictAllStateElements();
        }
        alertMeter.markEvent();
        out.collect(
            new Alert<>(
                rule.getRuleId(),
                rule,
                value.getKey(),
                value.getWrapped(),
                longToMoney(aggregateResult)));
      }
    }
  }

  @Override
  public void processBroadcastElement(
      Rule rule, Context ctx, Collector<Alert<Transaction, BigDecimal>> out) throws Exception {
    log.trace("Processing {}", rule);
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
      Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
    ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
          ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, Rule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.trace("Removed {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<Long> aggregator, Rule rule, Field aggregateField)
      throws Exception {
    Set<Transaction> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (int i = 0; i < inWindow.size(); ++i) {
        aggregator.add(1L);
      }
    } else {
      for (Transaction event : inWindow) {
        long aggregatedValue = FieldsExtractor.getByKeyAs(event, aggregateField);
        aggregator.add(aggregatedValue);
      }
    }
  }

  private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
      throws Exception {
    Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
    if (widestWindowRule != null && widestWindowRule.getRuleState() == Rule.RuleState.ACTIVE) {
      if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
        broadcastState.put(WIDEST_RULE_KEY, rule);
      }
    }
  }

  @Override
  public void onTimer(
      final long timestamp,
      final OnTimerContext ctx,
      final Collector<Alert<Transaction, BigDecimal>> out)
      throws Exception {

    Rule widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

    Optional<Long> cleanupEventTimeWindow =
        Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
    Optional<Long> cleanupEventTimeThreshold =
        cleanupEventTimeWindow.map(window -> timestamp - window);

    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}

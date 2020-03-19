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

import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.Transaction.PaymentType;
import com.ververica.field.sources.BaseGenerator;
import java.util.SplittableRandom;

public class TransactionsGenerator extends BaseGenerator<Transaction> {

  private static final long MAX_PAYEE_ID = 100000;
  private static final long MAX_BENEFICIARY_ID = 100000;

  private static final long MIN_PAYMENT_AMOUNT_CENTS = 5__00L;
  private static final long MAX_PAYMENT_AMOUNT_CENTS = 20__00L;

  public TransactionsGenerator(int maxRecordsPerSecond) {
    super(maxRecordsPerSecond);
  }

  @Override
  public Transaction randomEvent(SplittableRandom rnd, long id) {
    long transactionId = rnd.nextLong(Long.MAX_VALUE);
    long payeeId = rnd.nextLong(MAX_PAYEE_ID);
    long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
    long paymentAmount = rnd.nextLong(MIN_PAYMENT_AMOUNT_CENTS, MAX_PAYMENT_AMOUNT_CENTS);

    return Transaction.builder()
        .transactionId(transactionId)
        .payeeId(payeeId)
        .beneficiaryId(beneficiaryId)
        .paymentAmount(paymentAmount)
        .paymentType(paymentType(transactionId))
        .eventTime(System.currentTimeMillis())
        .ingestionTimestamp(System.currentTimeMillis())
        .build();
  }

  private PaymentType paymentType(long id) {
    int name = (int) (id % 2);
    switch (name) {
      case 0:
        return PaymentType.CRD;
      case 1:
        return PaymentType.CSH;
      default:
        throw new IllegalStateException("");
    }
  }
}

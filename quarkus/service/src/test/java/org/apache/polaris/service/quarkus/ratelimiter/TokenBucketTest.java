/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.quarkus.ratelimiter;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.service.ratelimiter.TokenBucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

/** Main unit test class for TokenBucket */
public class TokenBucketTest {
  @Test
  void testBasic() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneOffset.UTC);
    clock.add(Duration.ofSeconds(5));

    TokenBucket tokenBucket = new TokenBucket(10, 100, clock);

    assertCanAcquire(tokenBucket, 100);
    assertCannotAcquire(tokenBucket);

    clock.add(Duration.ofSeconds(1));
    assertCanAcquire(tokenBucket, 10);
    assertCannotAcquire(tokenBucket);

    clock.add(Duration.ofSeconds(10));
    assertCanAcquire(tokenBucket, 100);
    assertCannotAcquire(tokenBucket);
  }

  /**
   * Starts several threads that try to query the rate limiter at the same time, ensuring that we
   * only allow "maxTokens" requests
   */
  @Test
  @SuppressWarnings("FutureReturnValueIgnored") // implementation looks okay
  void testConcurrent() throws InterruptedException {
    int maxTokens = 100;
    int numTasks = 50000;
    int tokensPerSecond = 10; // Can be anything above 0

    TokenBucket rl =
        new TokenBucket(tokensPerSecond, maxTokens, Clock.fixed(Instant.now(), ZoneOffset.UTC));
    AtomicInteger numAcquired = new AtomicInteger();
    CountDownLatch startLatch = new CountDownLatch(numTasks);
    CountDownLatch endLatch = new CountDownLatch(numTasks);

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < numTasks; i++) {
        executor.submit(
            () -> {
              try {
                // Enforce that tasks pause until all tasks are submitted
                startLatch.countDown();
                startLatch.await();

                if (rl.tryAcquire()) {
                  numAcquired.incrementAndGet();
                }

                endLatch.countDown();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    endLatch.await();
    Assertions.assertEquals(maxTokens, numAcquired.get());
  }

  private void assertCanAcquire(TokenBucket tokenBucket, int times) {
    for (int i = 0; i < times; i++) {
      Assertions.assertTrue(tokenBucket.tryAcquire());
    }
  }

  private void assertCannotAcquire(TokenBucket tokenBucket) {
    for (int i = 0; i < 5; i++) {
      Assertions.assertFalse(tokenBucket.tryAcquire());
    }
  }
}

/*
 * Copyright © 2026 Valkyrien Skies contributors
 *
 * This file is part of Kwik, an implementation of the QUIC protocol in Java.
 *
 * Kwik is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * Kwik is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
 * more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package tech.kwik.core.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class SharedExecutorsTest {

    private final ExecutorService pool = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownPool() {
        pool.shutdownNow();
    }

    // region serial executors

    @Test
    void busyExecutorYieldsThePoolThreadToOthers() throws Exception {
        SharedExecutors.SerialExecutor busy = new SharedExecutors.SerialExecutor(pool);
        SharedExecutors.SerialExecutor other = new SharedExecutors.SerialExecutor(pool);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger busyRun = new AtomicInteger();
        AtomicInteger busyRunWhenOtherRan = new AtomicInteger(-1);
        CountDownLatch otherDone = new CountDownLatch(1);
        int busyTasks = 50 * SharedExecutors.SerialExecutor.BATCH;

        busy.execute(() -> await(gate));
        for (int i = 0; i < busyTasks; i++) {
            busy.execute(busyRun::incrementAndGet);
        }
        other.execute(() -> {
            busyRunWhenOtherRan.set(busyRun.get());
            otherDone.countDown();
        });
        gate.countDown();

        assertThat(otherDone.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(busyRunWhenOtherRan.get()).isLessThan(SharedExecutors.SerialExecutor.BATCH);
        busy.shutdown();
        assertThat(busy.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(busyRun.get()).isEqualTo(busyTasks);
    }

    @Test
    void tasksOfOneExecutorRunInOrderAcrossTurns() throws Exception {
        SharedExecutors.SerialExecutor serial = new SharedExecutors.SerialExecutor(pool);
        SharedExecutors.SerialExecutor other = new SharedExecutors.SerialExecutor(pool);
        List<Integer> order = new ArrayList<>();
        int count = 5 * SharedExecutors.SerialExecutor.BATCH + 3;
        for (int i = 0; i < count; i++) {
            int n = i;
            serial.execute(() -> order.add(n));
            other.execute(() -> { });
        }
        serial.shutdown();
        assertThat(serial.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            expected.add(i);
        }
        assertThat(order).isEqualTo(expected);
    }

    // endregion

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

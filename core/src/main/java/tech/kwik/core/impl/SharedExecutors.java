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

import tech.kwik.core.concurrent.DaemonThreadFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Opt-in sharing of timer and callback threads across connections (VS patch, not part of upstream Kwik).
 * <p>
 * By default every connection creates its own idle timer, loss detection scheduler and callback thread (plus a
 * datagram handler thread when no executor is given). A server with many connections then carries several threads
 * per connection. With {@link #enabled} set before connections are created, those use a few shared threads instead;
 * callbacks stay sequential per connection ({@link #serialCallbacks()}). The sender and receiver loops remain
 * per connection.
 */
public final class SharedExecutors {

    /** Set to true (before creating connections) to share timer and callback threads. */
    public static volatile boolean enabled = false;

    private static ScheduledThreadPoolExecutor timers;
    private static ThreadPoolExecutor callbacks;

    private SharedExecutors() {}

    /** The shared timer pool. Tasks must be cancelled, never the pool shut down. */
    public static synchronized ScheduledExecutorService timers() {
        if (timers == null) {
            timers = new ScheduledThreadPoolExecutor(2, new DaemonThreadFactory("kwik-shared-timer"));
            timers.setRemoveOnCancelPolicy(true);
            timers.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        }
        return timers;
    }

    private static synchronized ExecutorService callbacks() {
        if (callbacks == null) {
            callbacks = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                    new DaemonThreadFactory("kwik-shared-callbacks"), new ThreadPoolExecutor.DiscardPolicy());
        }
        return callbacks;
    }

    /** A new executor running its tasks one at a time, in order, on the shared callback threads. */
    public static ExecutorService serialCallbacks() {
        return new SerialExecutor(callbacks());
    }

    /**
     * Runs tasks sequentially on a shared pool. Like a single-thread executor: after {@link #shutdown()}, queued
     * tasks still run and new ones are discarded. A pool thread runs at most {@link #BATCH} tasks of one executor
     * before it moves to the back of the pool's queue, so one busy connection can't starve the others.
     */
    static final class SerialExecutor extends AbstractExecutorService {
        static final int BATCH = 64;

        private final ExecutorService pool;
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean running;
        private volatile boolean shutdown;

        SerialExecutor(ExecutorService pool) {
            this.pool = pool;
        }

        @Override
        public void execute(Runnable command) {
            synchronized (this) {
                if (shutdown) {
                    return;
                }
                tasks.add(command);
                if (running) {
                    return;
                }
                running = true;
            }
            pool.execute(this::drain);
        }

        private void drain() {
            for (int i = 0; i < BATCH; i++) {
                Runnable next;
                synchronized (this) {
                    next = tasks.poll();
                    if (next == null) {
                        running = false;
                        notifyAll();
                        return;
                    }
                }
                try {
                    next.run();
                }
                catch (RuntimeException e) {
                    // Like a thread pool: a failing task must not stop the ones after it.
                }
            }
            synchronized (this) {
                if (tasks.isEmpty()) {
                    running = false;
                    notifyAll();
                    return;
                }
            }
            // Still running: the remaining tasks continue in order once the pool gets back to this executor.
            pool.execute(this::drain);
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public synchronized List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> pending = List.copyOf(tasks);
            tasks.clear();
            return pending;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public synchronized boolean isTerminated() {
            return shutdown && !running && tasks.isEmpty();
        }

        @Override
        public synchronized boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (!isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return true;
        }
    }
}

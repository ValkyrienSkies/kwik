/*
 * Copyright © 2024, 2025, 2026 Peter Doornbosch
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
package tech.kwik.core;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Datagram extension as specified by RFC 9221 "An Unreliable Datagram Extension to QUIC".
 * https://www.rfc-editor.org/rfc/rfc9221.html#name-datagram-frame-types
 */
public interface DatagramExtension {

    /**
     * Returns whether the local endpoint can send datagrams (which includes whether the peer can receive them).
     * @return
     */
    boolean canSendDatagram();

    /**
     * Returns whether the local endpoint can receive datagrams (irrespective of whether the peer can send them).
     * @return
     */
    boolean canReceiveDatagram();

    /**
     * Returns whether the datagram extension is enabled for this connection, which means that the peer has indicated
     * support for the datagram extension and the local endpoint has enabled it ("can send", "can receive").
     * @return
     */
    boolean isDatagramExtensionEnabled();

    /**
     * Set the handler that will be called when a datagram is received.
     * The handler is called on a separate thread, but the same thread is used for all handler invocations, so handling
     * of datagrams is serialized.
     * @param handler
     */
    void setDatagramHandler(Consumer<byte[]> handler);

    /**
     * Set the handler that will be called when a datagram is received.
     * The handler is called on the provided executor.
     * @param handler
     * @param callbackExecutor
     */
    void setDatagramHandler(Consumer<byte[]> handler, ExecutorService callbackExecutor);

    /**
     * The maximum size of the data that can be sent in a single datagram.
     * @return
     */
    int maxDatagramDataSize();

    /**
     * Send a datagram.
     * @param data
     * @throws IllegalArgumentException if the data size exceeds the maximum datagram data size.
     */
    void sendDatagram(byte[] data);

    /**
     * Requests sending a datagram whose content is produced lazily, at the moment the connection is able to send
     * it (i.e. when congestion control allows a packet to be sent). This lets an application always send its
     * freshest data instead of queueing datagrams that are stale by the time the connection can send them.
     * <p>
     * The supplier is called (at most once per call of this method) on the sender thread, with the maximum number
     * of data bytes that fit; it returns the data to send (at most that many bytes) or {@code null} to send nothing.
     * (VS patch, not part of upstream Kwik.)
     *
     * @param supplier        produces the datagram data given the maximum data size, or returns null
     * @param minimumDataSize the supplier is only invoked when at least this many data bytes fit in a packet
     * @param lostCallback    called (on an internal thread) with the data when the packet carrying it is declared lost
     * @throws IllegalStateException if the datagram extension is not enabled for sending
     */
    void sendDatagram(IntFunction<byte[]> supplier, int minimumDataSize, Consumer<byte[]> lostCallback);
}

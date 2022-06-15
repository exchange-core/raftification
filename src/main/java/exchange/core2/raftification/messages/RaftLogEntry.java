/*
 * Copyright 2021-2022 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package exchange.core2.raftification.messages;

import exchange.core2.raftification.RsmRequestFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public record RaftLogEntry<T extends RsmCommand>(int term, T cmd, long timestamp) {

    public static <T extends RsmCommand> RaftLogEntry<T> create(ByteBuffer buffer,
                                                                RsmRequestFactory<T, ?> factory) {
        final int term = buffer.getInt();
        final long timestamp = buffer.getLong();
        final T cmd = factory.createCommand(buffer);
        return new RaftLogEntry<>(term, cmd, timestamp);
    }

    public static <T extends RsmCommand> RaftLogEntry<T> create(DataInputStream dis,
                                                                RsmRequestFactory<T, ?> factory) throws IOException {
        final int term = dis.readInt();
        final long timestamp = dis.readLong();
        final T cmd = factory.createCommand(dis);
        return new RaftLogEntry<>(term, cmd, timestamp);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putLong(timestamp);
        cmd.serialize(buffer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RaftLogEntry<?> that = (RaftLogEntry<?>) o;
        return term == that.term && timestamp == that.timestamp && Objects.equals(cmd, that.cmd);
    }

    @Override
    public int hashCode() {
        // timestamp is assigned by leader - so it should be included
        return Objects.hash(term, cmd, timestamp);
    }

    @Override
    public String toString() {
        return "RLE{" +
                "term=" + term +
                " cmd=" + cmd +
                '}';
    }

}

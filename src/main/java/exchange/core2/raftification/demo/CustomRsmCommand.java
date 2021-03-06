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

package exchange.core2.raftification.demo;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public record CustomRsmCommand(long data) implements ICustomRsmCommand {

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    @Override
    public String toString() {
        return "CRC{" +
                "data=" + data +
                '}';
    }

    public static CustomRsmCommand create(ByteBuffer buffer) {
        return new CustomRsmCommand(buffer.getLong());
    }

    public static CustomRsmCommand create(DataInputStream dis) throws IOException {
        return new CustomRsmCommand(dis.readLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final CustomRsmCommand that = (CustomRsmCommand) o;
        return data == that.data;
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}

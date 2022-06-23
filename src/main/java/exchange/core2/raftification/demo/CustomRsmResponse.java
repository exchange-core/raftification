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

import java.nio.ByteBuffer;

public record CustomRsmResponse(int hash, long lastData) implements ICustomRsmResponse {

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(hash);
        buffer.putLong(lastData);
    }

    @Override
    public String toString() {
        return "CRR{" +
                "hash=" + hash +
                ", lastData=" + lastData +
                '}';
    }

    public static CustomRsmResponse create(ByteBuffer buffer) {
        return new CustomRsmResponse(
                buffer.getInt(),
                buffer.getLong());
    }
}

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

// TODO support batching !!

public record CustomQuery<Q extends RsmQuery>(Q rsmQuery) implements RpcRequest {

    @Override
    public int getMessageType() {
        return QUERY_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        rsmQuery.serialize(buffer);
    }

    public static <Q extends RsmQuery> CustomQuery<Q> create(
            final ByteBuffer buffer,
            final RsmRequestFactory<?, Q> factory) {

        return new CustomQuery<>(factory.createQuery(buffer));
    }


    public static <Q extends RsmQuery> CustomQuery<Q> create(
            final DataInputStream dis,
            final RsmRequestFactory<?, Q> factory) throws IOException {

        return new CustomQuery<>(factory.createQuery(dis));
    }
}
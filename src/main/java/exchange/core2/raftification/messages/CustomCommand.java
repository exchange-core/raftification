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

public record CustomCommand<T extends RsmCommand>(T rsmCommand) implements RpcRequest {

    @Override
    public int getMessageType() {
        return COMMAND_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        rsmCommand.serialize(buffer);
    }

    public static <T extends RsmCommand> CustomCommand<T> create(
            final ByteBuffer buffer,
            final RsmRequestFactory<T, ?> factory) {

        return new CustomCommand<>(factory.createCommand(buffer));
    }


    public static <T extends RsmCommand> CustomCommand<T> create(
            final DataInputStream dis,
            final RsmRequestFactory<T, ?> factory) throws IOException {

        return new CustomCommand<>(factory.createCommand(dis));
    }
}

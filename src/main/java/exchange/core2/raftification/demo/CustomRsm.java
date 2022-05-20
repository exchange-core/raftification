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

import exchange.core2.raftification.ReplicatedStateMachine;
import exchange.core2.raftification.RsmRequestFactory;
import exchange.core2.raftification.RsmResponseFactory;
import net.openhft.chronicle.bytes.BytesOut;
import org.agrona.collections.Hashing;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CustomRsm implements
        ReplicatedStateMachine<CustomRsmCommand, CustomRsmResponse>,
        RsmRequestFactory<CustomRsmCommand>,
        RsmResponseFactory<CustomRsmResponse> {

    public static final CustomRsmResponse EMPTY_RSM_RESPONSE = new CustomRsmResponse(0);

    // state
    private int hash = 0;

    @Override
    public CustomRsmResponse applyCommand(CustomRsmCommand cmd) {
        hash = Hashing.hash(hash ^ Hashing.hash(cmd.data()));
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmResponse applyQuery(CustomRsmCommand query) {
        // can not change anything
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmCommand createRequest(ByteBuffer buffer) {
        return CustomRsmCommand.create(buffer);
    }

    @Override
    public CustomRsmCommand createRequest(DataInputStream dis) throws IOException {

        return CustomRsmCommand.create(dis);
    }

    @Override
    public CustomRsmResponse createResponse(ByteBuffer buffer) {
        return CustomRsmResponse.create(buffer);
    }

    @Override
    public CustomRsmResponse emptyResponse() {
        return EMPTY_RSM_RESPONSE;
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.append(hash);
    }
}

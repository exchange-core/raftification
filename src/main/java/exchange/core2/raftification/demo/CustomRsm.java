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
        ReplicatedStateMachine<ICustomRsmCommand, ICustomRsmQuery, ICustomRsmResponse>,
        RsmRequestFactory<ICustomRsmCommand, ICustomRsmQuery>,
        RsmResponseFactory<ICustomRsmResponse> {

    public static final ICustomRsmResponse EMPTY_RSM_RESPONSE = new CustomRsmResponse(0);

    // state
    private int hash = 0;
    private long lastData = 0L;

    public CustomRsm() {
        this.hash = 0;
        this.lastData = 0L;
    }


    public CustomRsm(int hash, long lastData) {
        this.hash = hash;
        this.lastData = lastData;
    }

    @Override
    public CustomRsmResponse applyCommand(ICustomRsmCommand cmd) {

        if (cmd instanceof CustomRsmCommand cmd1) {

            hash = Hashing.hash(hash ^ Hashing.hash(cmd1.data()));
            lastData = cmd1.data();
        } else {
            throw new IllegalStateException("Unknown command " + cmd);
        }

        return new CustomRsmResponse(hash);
    }

    @Override
    public ICustomRsmResponse applyQuery(ICustomRsmQuery query) {
        // can not change anything
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmCommand createCommand(ByteBuffer buffer) {
        return CustomRsmCommand.create(buffer);
    }

    @Override
    public CustomRsmCommand createCommand(DataInputStream dis) throws IOException {

        return CustomRsmCommand.create(dis);
    }

    @Override
    public ICustomRsmQuery createQuery(ByteBuffer buffer) {
        return null;
    }

    @Override
    public ICustomRsmQuery createQuery(DataInputStream dis) throws IOException {
        return null;
    }

    @Override
    public CustomRsmResponse createResponse(ByteBuffer buffer) {
        return CustomRsmResponse.create(buffer);
    }

    @Override
    public ICustomRsmResponse emptyResponse() {
        return EMPTY_RSM_RESPONSE;
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.append(hash); // int
        bytes.append(lastData); // long
    }
}

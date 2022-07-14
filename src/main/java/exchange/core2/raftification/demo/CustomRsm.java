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
import net.openhft.chronicle.bytes.BytesOut;
import org.agrona.collections.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CustomRsm implements
        ReplicatedStateMachine<ICustomRsmCommand, ICustomRsmQuery, ICustomRsmResponse> {

    private static final Logger log = LoggerFactory.getLogger(CustomRsm.class);

    // state
    private int hash;
    private long lastData;

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

            if (lastData + 1 == cmd1.data()) {
                final int newHash = Hashing.hash(hash ^ Hashing.hash(cmd1.data()));

                log.debug("RSM: {}->{} hash: {}->{}", lastData, cmd1.data(), hash, newHash);

                hash = newHash;
                lastData = cmd1.data();
            } else {

                log.warn("RSM: Ignoreed {}->{}", lastData, cmd1.data());

            }

        } else {
            throw new IllegalStateException("Unknown command " + cmd);
        }

        return new CustomRsmResponse(hash, lastData);
    }

    @Override
    public ICustomRsmResponse applyQuery(ICustomRsmQuery query) {
        // can not change anything
        return new CustomRsmResponse(hash, lastData);
    }


    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.append(hash); // int
        bytes.append(lastData); // long
    }

    @Override
    public String toString() {
        return "CustomRsm{" +
                "hash=" + hash +
                ", lastData=" + lastData +
                '}';
    }
}

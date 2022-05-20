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

package exchange.core2.raftification;

import exchange.core2.raftification.messages.RsmRequest;
import exchange.core2.raftification.messages.RsmResponse;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public interface ReplicatedStateMachine<T extends RsmRequest, S extends RsmResponse> extends WriteBytesMarshallable {

    /**
     * Changes state of Replicated State Machine
     *
     * @param command command
     * @return result
     */
    S applyCommand(T command);

    // TODO query

    /**
     * Execute a query that does not change the state
     *
     * @param query query
     * @return query result
     */
    S applyQuery(T query);

}

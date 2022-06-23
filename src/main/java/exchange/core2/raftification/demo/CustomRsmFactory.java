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

import exchange.core2.raftification.RsmRequestFactory;
import exchange.core2.raftification.RsmResponseFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CustomRsmFactory implements
        RsmRequestFactory<ICustomRsmCommand, ICustomRsmQuery>,
        RsmResponseFactory<ICustomRsmResponse> {

    public static final ICustomRsmResponse EMPTY_RSM_RESPONSE = new CustomRsmResponse(0, 0L);

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

}

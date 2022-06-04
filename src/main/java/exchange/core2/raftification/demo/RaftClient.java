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


import exchange.core2.raftification.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class RaftClient {

    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        final RaftClient raftClient = new RaftClient();

        final Random random = new Random(1L);
        int counter = 1;
        long val = 1_000_000L;
        while (true) {

            final boolean success = raftClient.sendCommand(val, counter);

            if (success) {
                counter++;
                val = counter * 1_000_000L + random.nextInt(1_000_000);
            }


            Thread.sleep(2000);
        }
    }

    private final RpcClient<CustomRsmCommand, CustomRsmResponse> rpcClient;

    public RaftClient() {

        // localhost:3778, localhost:3779, localhost:3780
        final Map<Integer, String> remoteNodes = Map.of(
                0, "localhost:3778",
                1, "localhost:3779",
                2, "localhost:3780");

        this.rpcClient = new RpcClient<>(remoteNodes, new CustomRsm());
    }

    public boolean sendCommand(long data, int counter) {
        try {
            log.info("send >>> {} data={}", counter, data);
            final CustomRsmResponse res = rpcClient.callRpcSync(new CustomRsmCommand(data), 500);
            log.info("recv <<< hash={}", res.hash());
            return true;
        } catch (Exception ex) {
            log.warn("Exception: ", ex);
        }
        return false;
    }

}
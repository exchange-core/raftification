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
import exchange.core2.raftification.messages.NodeStatusRequest;
import exchange.core2.raftification.messages.NodeStatusResponse;
import exchange.core2.raftification.messages.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CustomRaftClient {

    private static final Logger log = LoggerFactory.getLogger(CustomRaftClient.class);

    private final List<String> remoteNodes;
    private final RpcClient<ICustomRsmCommand, ICustomRsmQuery, ICustomRsmResponse> rpcClient;

    private final CustomRsm localRsm;

    private long counter = 1L;

    public static void main(String[] args) throws InterruptedException {


        // localhost:3778, localhost:3779, localhost:3780
        final List<String> remoteNodes = List.of(
                "localhost:3778",
                "localhost:3779",
                "localhost:3780");

        final CustomRaftClient customRaftClient = new CustomRaftClient(remoteNodes);

        customRaftClient.runTest();

    }

    public CustomRaftClient(final List<String> remoteNodes) {
        this.remoteNodes = remoteNodes;
        this.localRsm = new CustomRsm();

        final CustomRsmFactory customRsmFactory = new CustomRsmFactory();

        this.rpcClient = new RpcClient<>(remoteNodes, customRsmFactory);
    }


    public void runTest() throws InterruptedException {

//        verifyClusterState();

        final CustomRsmQuery query = new CustomRsmQuery();


        while (true) {


            log.info("send >>> {}", counter);

            final CustomRsmCommand cmd = new CustomRsmCommand(counter);
            final Optional<CustomRsmResponse> responseOpt = sendCommand(cmd);
            log.info("recv <<< {}", responseOpt);

            responseOpt.ifPresent(remoteRes -> {

                final int localRes = localRsm.applyCommand(cmd).hash();

                if (remoteRes.lastData() != counter) {
                    log.warn("received counter {}, last sent {}", remoteRes.lastData(), counter);
                    counter = remoteRes.lastData() + 1;

                } else {

                    counter++;

                    if (localRes != remoteRes.hash()) {
                        throw new IllegalStateException("Expected " + localRes + " but received " + remoteRes);
                    }
                }
            });

            Thread.sleep(1000);
//            customRaftClient.verifyClusterState();
//            Thread.sleep(1000);
        }
    }

    public Optional<CustomRsmResponse> sendCommand(CustomRsmCommand cmd) {
        try {
            final ICustomRsmResponse res = rpcClient.callRpcSync(cmd, 100);
            if (res instanceof CustomRsmResponse res1) {
                return Optional.of(res1);
            } else {
                throw new IllegalStateException("Unexpected response: " + res);
            }
        } catch (Exception ex) {
//            log.warn("Exception: {} {}", ex.getClass().getSimpleName(), ex.getMessage());
            log.warn("Exception: ", ex);
        }

        return Optional.empty();
    }

    public boolean verifyClusterState() {

        for (int attempt = 1; attempt <= 2; attempt++) {

            log.debug("verifyClusterState: attempt={}", attempt);

            final List<CompletableFuture<NodeStatusResponse>> futures = IntStream.range(0, 3)
                    .mapToObj(nodeId -> (CompletableFuture<NodeStatusResponse>) rpcClient.callRpcAsync(nodeId, new NodeStatusRequest()))
                    .collect(Collectors.toList());

            CompletableFuture<NodeStatusResponse> firstLeaderFuture = firstMatching(futures, NodeStatusResponse::isLeader);

            try {
                final RpcResponse rpcResponse = firstLeaderFuture.get(500, TimeUnit.MILLISECONDS);

                log.debug("rpcResponse: {}", rpcResponse);

                return true;

            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        return false;
    }


    public static <T> CompletableFuture<T> firstMatching(final Collection<CompletableFuture<T>> futures,
                                                         final Predicate<? super T> predicate) {

        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        final AtomicInteger counter = new AtomicInteger();

        futures.forEach(future ->
                future.thenAccept(data -> {
                    final int numCompleted = counter.incrementAndGet();
                    if (predicate.test(data)) {
                        resultFuture.complete(data);
                    } else if (numCompleted == futures.size()) {
                        resultFuture.completeExceptionally(new RuntimeException("No futures match to predicate"));
                    }
                }));

        return resultFuture;
    }

}
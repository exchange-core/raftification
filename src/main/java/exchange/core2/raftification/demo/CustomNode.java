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

import exchange.core2.raftification.RaftNode;
import exchange.core2.raftification.repository.RaftDiskLogConfig;
import exchange.core2.raftification.repository.RaftDiskLogRepository;

import java.nio.file.Path;
import java.util.List;

public class CustomNode {

    public static void main(String[] args) {

        final int thisNodeId = Integer.parseInt(args[0]);

        final CustomRsm customRsm = new CustomRsm();
        final CustomRsmFactory customRsmFactory = new CustomRsmFactory();

        final Path folder = Path.of("./raftlogs/node" + thisNodeId);

        // localhost:3778, localhost:3779, localhost:3780
        final List<String> remoteNodes = List.of(
                "localhost:3778",
                "localhost:3779",
                "localhost:3780");

        final RaftDiskLogConfig raftDiskLogConfig = new RaftDiskLogConfig(folder, "EC2RT");

        final RaftDiskLogRepository<ICustomRsmCommand> repository = new RaftDiskLogRepository<>(customRsmFactory, raftDiskLogConfig);
//        final IRaftLogRepository<CustomRsmCommand> repository = new RaftMemLogRepository<>();

        new RaftNode<>(remoteNodes, thisNodeId, repository, customRsm, customRsmFactory, customRsmFactory);
    }

}

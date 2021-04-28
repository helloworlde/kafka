/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.replica;

import org.apache.kafka.common.Node;

import java.util.Comparator;
import java.util.Objects;

/**
 * View of a replica used by {@link ReplicaSelector} to determine a preferred replica.
 */
public interface ReplicaView {

    /**
     * The endpoint information for this replica (hostname, port, rack, etc)
     */
    Node endpoint();

    /**
     * The log end offset for this replica
     */
    long logEndOffset();

    /**
     * The number of milliseconds (if any) since the last time this replica was caught up to the high watermark.
     * For a leader replica, this is always zero.
     */
    long timeSinceLastCaughtUpMs();

    /**
     * Comparator for ReplicaView that returns in the order of "most caught up". This is used for deterministic
     * selection of a replica when there is a tie from a selector.
     *
     * 返回最同步的副本
     */
    static Comparator<ReplicaView> comparator() {
        // 先比较日志的 offset，然后比较落后 leader 的时间，然后比较副本的 id
        // 即优先选择和 leader 节点同步 offset 接近的；
        // 如果 offset 同步一致，则选择和 leader 同步间隔最小的
        // 如果都一致，则选择副本 id 最大的
        return Comparator.comparingLong(ReplicaView::logEndOffset)
            .thenComparing(Comparator.comparingLong(ReplicaView::timeSinceLastCaughtUpMs).reversed())
            .thenComparing(replicaInfo -> replicaInfo.endpoint().id());
    }

    class DefaultReplicaView implements ReplicaView {
        private final Node endpoint;
        private final long logEndOffset;
        private final long timeSinceLastCaughtUpMs;

        public DefaultReplicaView(Node endpoint, long logEndOffset, long timeSinceLastCaughtUpMs) {
            this.endpoint = endpoint;
            this.logEndOffset = logEndOffset;
            this.timeSinceLastCaughtUpMs = timeSinceLastCaughtUpMs;
        }

        @Override
        public Node endpoint() {
            return endpoint;
        }

        @Override
        public long logEndOffset() {
            return logEndOffset;
        }

        @Override
        public long timeSinceLastCaughtUpMs() {
            return timeSinceLastCaughtUpMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DefaultReplicaView that = (DefaultReplicaView) o;
            return logEndOffset == that.logEndOffset &&
                    Objects.equals(endpoint, that.endpoint) &&
                    Objects.equals(timeSinceLastCaughtUpMs, that.timeSinceLastCaughtUpMs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoint, logEndOffset, timeSinceLastCaughtUpMs);
        }

        @Override
        public String toString() {
            return "DefaultReplicaView{" +
                    "endpoint=" + endpoint +
                    ", logEndOffset=" + logEndOffset +
                    ", timeSinceLastCaughtUpMs=" + timeSinceLastCaughtUpMs +
                    '}';
        }
    }
}

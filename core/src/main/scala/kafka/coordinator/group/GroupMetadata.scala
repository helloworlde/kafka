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
package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import kafka.utils.Implicits._
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import scala.collection.{Seq, immutable, mutable}
import scala.jdk.CollectionConverters._

private[group] sealed trait GroupState {
  val validPreviousStates: Set[GroupState]
}

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, CompletingRebalance, Empty)
}

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(CompletingRebalance)
}

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead)
}

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState {
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}


private object GroupMetadata extends Logging {

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocolName: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocolName = Option(protocolName)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach(member => {
      group.add(member, null)
      if (member.isStaticMember) {
        info(s"Static member $member.groupInstanceId of group $groupId loaded " +
          s"with member id ${member.memberId} at generation ${group.generationId}.")
        group.addStaticMember(member.groupInstanceId, member.memberId)
      }
    })
    group.subscribedTopics = group.computeSubscribedTopics()
    group
  }

  private val MemberIdDelimiter = "-"
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String,
                         state: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic itself may result in the wrong offset commit
  * being materialized.
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset): Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  private var state: GroupState = initialState
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  var protocolName: Option[String] = None
  var generationId = 0
  private var leaderId: Option[String] = None

  private val members = new mutable.HashMap[String, MemberMetadata]
  // Static membership mapping [key: group.instance.id, value: member.id]
  private val staticMembers = new mutable.HashMap[String, String]
  private val pendingMembers = new mutable.HashSet[String]
  private var numMembersAwaitingJoin = 0
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  // When protocolType == `consumer`, a set of subscribed topics is maintained. The set is
  // computed when a new generation is created or when the group is restored from the log.
  private var subscribedTopics: Option[Set[String]] = None

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)
  def size = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def isConsumerGroup: Boolean = protocolType.contains(ConsumerProtocol.PROTOCOL_TYPE)

  // 添加成员
  def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    assert(this.protocolType.orNull == member.protocolType)
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))

    // 如果没有 leader，则当前成员成为 leader
    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)

    // 添加到集合中
    members.put(member.memberId, member)
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    member.awaitingJoinCallback = callback
    if (member.isAwaitingJoin)
      numMembersAwaitingJoin += 1
  }

  def remove(memberId: String): Unit = {
    members.remove(memberId).foreach { member =>
      member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1
    }

    if (isLeader(memberId))
      leaderId = members.keys.headOption
  }

  /**
    * Check whether current leader is rejoined. If not, try to find another joined member to be
    * new leader. Return false if
    *   1. the group is currently empty (has no designated leader)
    *   2. no member rejoined
    */
  def maybeElectNewJoinedLeader(): Boolean = {
    leaderId.exists { currentLeaderId =>
      val currentLeader = get(currentLeaderId)
      if (!currentLeader.isAwaitingJoin) {
        members.find(_._2.isAwaitingJoin) match {
          case Some((anyJoinedMemberId, anyJoinedMember)) =>
            leaderId = Option(anyJoinedMemberId)
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, while new leader $anyJoinedMember was elected.")
            true

          case None =>
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, and the group couldn't proceed to next generation" +
              s"because no member joined.")
            false
        }
      } else {
        true
      }
    }
  }

  /**
    * [For static members only]: Replace the old member id with the new one,
    * keep everything else unchanged and return the updated member.
    */
  def replaceGroupInstance(oldMemberId: String,
                           newMemberId: String,
                           groupInstanceId: Option[String]): MemberMetadata = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in replaceGroupInstance")
    }
    val oldMember = members.remove(oldMemberId)
      .getOrElse(throw new IllegalArgumentException(s"Cannot replace non-existing member id $oldMemberId"))

    // Fence potential duplicate member immediately if someone awaits join/sync callback.
    maybeInvokeJoinCallback(oldMember, JoinGroupResult(oldMemberId, Errors.FENCED_INSTANCE_ID))

    maybeInvokeSyncCallback(oldMember, SyncGroupResult(Errors.FENCED_INSTANCE_ID))

    oldMember.memberId = newMemberId
    members.put(newMemberId, oldMember)

    if (isLeader(oldMemberId))
      leaderId = Some(newMemberId)
    addStaticMember(groupInstanceId, newMemberId)
    oldMember
  }

  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId) && !has(memberId)

  def addPendingMember(memberId: String) = pendingMembers.add(memberId)

  def removePendingMember(memberId: String) = pendingMembers.remove(memberId)

  def hasStaticMember(groupInstanceId: Option[String]) = groupInstanceId.isDefined && staticMembers.contains(groupInstanceId.get)

  def getStaticMemberId(groupInstanceId: Option[String]) = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in getStaticMemberId")
    }
    staticMembers(groupInstanceId.get)
  }

  def addStaticMember(groupInstanceId: Option[String], newMemberId: String) = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in addStaticMember")
    }
    staticMembers.put(groupInstanceId.get, newMemberId)
  }

  def removeStaticMember(groupInstanceId: Option[String]) = {
    if (groupInstanceId.isDefined) {
      staticMembers.remove(groupInstanceId.get)
    }
  }

  def currentState = state

  def notYetRejoinedMembers = members.filter(!_._2.isAwaitingJoin).toMap

  def hasAllMembersJoined = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers = members.keySet

  def allStaticMembers = staticMembers.keySet

  // For testing only.
  def allDynamicMembers = {
    val dynamicMemberSet = new mutable.HashSet[String]
    allMembers.foreach(memberId => dynamicMemberSet.add(memberId))
    staticMembers.values.foreach(memberId => dynamicMemberSet.remove(memberId))
    dynamicMemberSet.toSet
  }

  def numPending = pendingMembers.size

  def numAwaiting: Int = numMembersAwaitingJoin

  def allMemberMetadata = members.values.toList

  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  /**
   * 使用 UUID 生成一个成员 id
   */
  def generateMemberId(clientId: String,
                       groupInstanceId: Option[String]): String = {
    groupInstanceId match {
      case None =>
        clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
      case Some(instanceId) =>
        instanceId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
    }
  }

  /**
   * Verify the member.id is up to date for static members. Return true if both conditions met:
   *   1. given member is a known static member to group
   *   2. group stored member.id doesn't match with given member.id
   * 验证静态成员的 member.id，如果满足以下两个条件，则返回 true:
   *   1. member 是已知的 Group 静态成员
   *   2. Group 存储的 member.id 和所给的不一致
   */
  def isStaticMemberFenced(memberId: String,
                           groupInstanceId: Option[String],
                           operation: String): Boolean = {
    if (hasStaticMember(groupInstanceId)
      && getStaticMemberId(groupInstanceId) != memberId) {
      error(s"given member.id $memberId is identified as a known static member ${groupInstanceId.get}, " +
        s"but not matching the expected member.id ${getStaticMemberId(groupInstanceId)} during $operation, will " +
        s"respond with instance fenced error")
      true
    } else
      false
  }

  def canRebalance = PreparingRebalance.validPreviousStates.contains(state)

  def transitionTo(groupState: GroupState): Unit = {
    assertValidTransition(groupState)
    state = groupState
    currentStateTimestamp = Some(time.milliseconds())
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }

    protocol
  }

  private def candidateProtocols: Set[String] = {
    // get the set of protocols that are commonly supported by all members
    val numMembers = members.size
    supportedProtocols.filter(_._2 == numMembers).map(_._1).toSet
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]): Boolean = {
    if (is(Empty))
      !memberProtocolType.isEmpty && memberProtocols.nonEmpty
    else
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
  }

  def getSubscribedTopics: Option[Set[String]] = subscribedTopics

  /**
   * Returns true if the consumer group is actively subscribed to the topic. When the consumer
   * group does not know, because the information is not available yet or because the it has
   * failed to parse the Consumer Protocol, it returns true to be safe.
   */
  def isSubscribedToTopic(topic: String): Boolean = subscribedTopics match {
    case Some(topics) => topics.contains(topic)
    case None => true
  }

  /**
   * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
   * to 'consumer'. None is returned if
   * - the protocol type is not equal to 'consumer';
   * - the protocol is not defined yet; or
   * - the protocol metadata does not comply with the schema.
   */
  private[group] def computeSubscribedTopics(): Option[Set[String]] = {
    protocolType match {
      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.nonEmpty && protocolName.isDefined =>
        try {
          Some(
            members.map { case (_, member) =>
              // The consumer protocol is parsed with V0 which is the based prefix of all versions.
              // This way the consumer group manager does not depend on any specific existing or
              // future versions of the consumer protocol. VO must prefix all new versions.
              val buffer = ByteBuffer.wrap(member.metadata(protocolName.get))
              ConsumerProtocol.deserializeVersion(buffer)
              ConsumerProtocol.deserializeSubscription(buffer, 0).topics.asScala.toSet
            }.reduceLeft(_ ++ _)
          )
        } catch {
          case e: SchemaException =>
            warn(s"Failed to parse Consumer Protocol ${ConsumerProtocol.PROTOCOL_TYPE}:${protocolName.get} " +
              s"of group $groupId. Consumer group coordinator is not aware of the subscribed topics.", e)
            None
        }

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.isEmpty =>
        Option(Set.empty)

      case _ => None
    }
  }

  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback): Unit = {
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
    protocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    member.supportedProtocols = protocols

    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    member.awaitingJoinCallback = callback
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult): Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  /**
    * @return true if a sync callback actually performs.
    */
  def maybeInvokeSyncCallback(member: MemberMetadata,
                              syncGroupResult: SyncGroupResult): Boolean = {
    if (member.isAwaitingSync) {
      member.awaitingSyncCallback(syncGroupResult)
      member.awaitingSyncCallback = null
      true
    } else {
      false
    }
  }

  def initNextGeneration(): Unit = {
    if (members.nonEmpty) {
      generationId += 1
      protocolName = Some(selectProtocol)
      subscribedTopics = computeSubscribedTopics()
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocolName = None
      subscribedTopics = computeSubscribedTopics()
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: List[JoinGroupResponseMember] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => new JoinGroupResponseMember()
        .setMemberId(memberId)
        .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
        .setMetadata(memberMetadata.metadata(protocolName.get))
    }.toList
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolName.orNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""), state.toString)
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  // 消息提交回调，会更新 offsets 缓存，将 Partition 从待提交中移除
  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset): Unit = {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      // 更新 offsets 缓存信息
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    // 将 Partition 从待提交的集合中移除
    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  // offset 提交
  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.forKeyValue { (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers: collection.Set[Long] = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long): Boolean =
    pendingTransactionalOffsetCommits.contains(producerId)

  def hasPendingOffsetCommitsForTopicPartition(topicPartition: TopicPartition): Boolean = {
    pendingOffsetCommits.contains(topicPartition) ||
      pendingTransactionalOffsetCommits.exists(
        _._2.contains(topicPartition)
      )
  }

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.forKeyValue { (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long): Map[TopicPartition, OffsetAndMetadata] = {

    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long,
                          subscribedTopics: Set[String] = Set.empty): Map[TopicPartition, OffsetAndMetadata] = {
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !subscribedTopics.contains(topicPartition.topic()) &&
          !pendingOffsetCommits.contains(topicPartition) && {
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(
          commitRecordMetadataAndOffset => currentStateTimestamp
            .getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp)
        )

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if subscribedTopics.isDefined =>
        // consumers exist in the group =>
        // - if the group is aware of the subscribed topics and retention period had passed since the
        //   the last commit timestamp, expire the offset. offset with pending offset commit are not
        //   expired
        getExpiredOffsets(
          _.offsetAndMetadata.commitTimestamp,
          subscribedTopics.get
        )

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")

    offsets --= expiredOffsets.keySet
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState): Unit = {
    if (!targetState.validPreviousStates.contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, targetState.validPreviousStates.mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}


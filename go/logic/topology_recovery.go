/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package logic

import (
	"encoding/json"
	"fmt"
	"math/rand"
	goos "os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/orchestrator/go/attributes"
	"github.com/github/orchestrator/go/config"
	"github.com/github/orchestrator/go/inst"
	"github.com/github/orchestrator/go/kv"
	ometrics "github.com/github/orchestrator/go/metrics"
	"github.com/github/orchestrator/go/os"
	"github.com/github/orchestrator/go/process"
	"github.com/github/orchestrator/go/raft"
	"github.com/github/orchestrator/go/util"
	"github.com/openark/golib/log"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

var countPendingRecoveries int64

type RecoveryType string

const (
	MainRecovery             RecoveryType = "MainRecovery"
	CoMainRecovery                        = "CoMainRecovery"
	IntermediateMainRecovery              = "IntermediateMainRecovery"
)

type RecoveryAcknowledgement struct {
	CreatedAt time.Time
	Owner     string
	Comment   string

	Key           inst.InstanceKey
	ClusterName   string
	Id            int64
	UID           string
	AllRecoveries bool
}

func NewRecoveryAcknowledgement(owner string, comment string) *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     owner,
		Comment:   comment,
	}
}

func NewInternalAcknowledgement() *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     "orchestrator",
		Comment:   "internal",
	}
}

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	Id                        int64
	UID                       string
	AnalysisEntry             inst.ReplicationAnalysis
	SuccessorKey              *inst.InstanceKey
	SuccessorAlias            string
	IsActive                  bool
	IsSuccessful              bool
	LostReplicas              inst.InstanceKeyMap
	ParticipatingInstanceKeys inst.InstanceKeyMap
	AllErrors                 []string
	RecoveryStartTimestamp    string
	RecoveryEndTimestamp      string
	ProcessingNodeHostname    string
	ProcessingNodeToken       string
	Acknowledged              bool
	AcknowledgedAt            string
	AcknowledgedBy            string
	AcknowledgedComment       string
	LastDetectionId           int64
	RelatedRecoveryId         int64
	Type                      RecoveryType
	RecoveryType              MainRecoveryType
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = util.PrettyUniqueToken()
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostReplicas = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.RecoveryType = NotMainRecovery
	return topologyRecovery
}

func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type TopologyRecoveryStep struct {
	Id          int64
	RecoveryUID string
	AuditAt     string
	Message     string
}

func NewTopologyRecoveryStep(uid string, message string) *TopologyRecoveryStep {
	return &TopologyRecoveryStep{
		RecoveryUID: uid,
		Message:     message,
	}
}

type MainRecoveryType string

const (
	NotMainRecovery          MainRecoveryType = "NotMainRecovery"
	MainRecoveryGTID                            = "MainRecoveryGTID"
	MainRecoveryPseudoGTID                      = "MainRecoveryPseudoGTID"
	MainRecoveryBinlogServer                    = "MainRecoveryBinlogServer"
)

var emergencyReadTopologyInstanceMap *cache.Cache
var emergencyRestartReplicaTopologyInstanceMap *cache.Cache
var emergencyOperationGracefulPeriodMap *cache.Cache

// InstancesByCountReplicas sorts instances by umber of replicas, descending
type InstancesByCountReplicas [](*inst.Instance)

func (this InstancesByCountReplicas) Len() int      { return len(this) }
func (this InstancesByCountReplicas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountReplicas) Less(i, j int) bool {
	if len(this[i].SubordinateHosts) == len(this[j].SubordinateHosts) {
		// Secondary sorting: prefer more advanced replicas
		return !this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
	}
	return len(this[i].SubordinateHosts) < len(this[j].SubordinateHosts)
}

var recoverDeadMainCounter = metrics.NewCounter()
var recoverDeadMainSuccessCounter = metrics.NewCounter()
var recoverDeadMainFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMainCounter = metrics.NewCounter()
var recoverDeadIntermediateMainSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMainFailureCounter = metrics.NewCounter()
var recoverDeadCoMainCounter = metrics.NewCounter()
var recoverDeadCoMainSuccessCounter = metrics.NewCounter()
var recoverDeadCoMainFailureCounter = metrics.NewCounter()
var countPendingRecoveriesGauge = metrics.NewGauge()

func init() {
	metrics.Register("recover.dead_main.start", recoverDeadMainCounter)
	metrics.Register("recover.dead_main.success", recoverDeadMainSuccessCounter)
	metrics.Register("recover.dead_main.fail", recoverDeadMainFailureCounter)
	metrics.Register("recover.dead_intermediate_main.start", recoverDeadIntermediateMainCounter)
	metrics.Register("recover.dead_intermediate_main.success", recoverDeadIntermediateMainSuccessCounter)
	metrics.Register("recover.dead_intermediate_main.fail", recoverDeadIntermediateMainFailureCounter)
	metrics.Register("recover.dead_co_main.start", recoverDeadCoMainCounter)
	metrics.Register("recover.dead_co_main.success", recoverDeadCoMainSuccessCounter)
	metrics.Register("recover.dead_co_main.fail", recoverDeadCoMainFailureCounter)
	metrics.Register("recover.pending", countPendingRecoveriesGauge)

	go initializeTopologyRecoveryPostConfiguration()

	ometrics.OnMetricsTick(func() {
		countPendingRecoveriesGauge.Update(getCountPendingRecoveries())
	})
}

func getCountPendingRecoveries() int64 {
	return atomic.LoadInt64(&countPendingRecoveries)
}

func initializeTopologyRecoveryPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	emergencyReadTopologyInstanceMap = cache.New(time.Second, time.Millisecond*250)
	emergencyRestartReplicaTopologyInstanceMap = cache.New(time.Second*30, time.Second)
	emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
}

// AuditTopologyRecovery audits a single step in a topology recovery process.
func AuditTopologyRecovery(topologyRecovery *TopologyRecovery, message string) error {
	log.Infof("topology_recovery: %s", message)
	if topologyRecovery == nil {
		return nil
	}

	recoveryStep := NewTopologyRecoveryStep(topologyRecovery.UID, message)
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("write-recovery-step", recoveryStep)
		return err
	} else {
		return writeTopologyRecoveryStep(recoveryStep)
	}
}

func resolveRecovery(topologyRecovery *TopologyRecovery, successorInstance *inst.Instance) error {
	if successorInstance != nil {
		topologyRecovery.SuccessorKey = &successorInstance.Key
		topologyRecovery.SuccessorAlias = successorInstance.InstanceAlias
		topologyRecovery.IsSuccessful = true
	}
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("resolve-recovery", topologyRecovery)
		return err
	} else {
		return writeResolveRecovery(topologyRecovery)
	}
}

// replaceCommandPlaceholders replaces agreed-upon placeholders with analysis data
func replaceCommandPlaceholders(command string, topologyRecovery *TopologyRecovery) string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	command = strings.Replace(command, "{failureType}", string(analysisEntry.Analysis), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{command}", analysisEntry.CommandHint, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{failureClusterDomain}", analysisEntry.ClusterDetails.ClusterDomain, -1)
	command = strings.Replace(command, "{countSubordinates}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{countReplicas}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoMainRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedMainRecovery), -1)
	command = strings.Replace(command, "{autoIntermediateMainRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediateMainRecovery), -1)
	command = strings.Replace(command, "{orchestratorHost}", process.ThisHostname, -1)
	command = strings.Replace(command, "{recoveryUID}", topologyRecovery.UID, -1)

	command = strings.Replace(command, "{isSuccessful}", fmt.Sprint(topologyRecovery.SuccessorKey != nil), -1)
	if topologyRecovery.SuccessorKey != nil {
		command = strings.Replace(command, "{successorHost}", topologyRecovery.SuccessorKey.Hostname, -1)
		command = strings.Replace(command, "{successorPort}", fmt.Sprintf("%d", topologyRecovery.SuccessorKey.Port), -1)
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		command = strings.Replace(command, "{successorAlias}", topologyRecovery.SuccessorAlias, -1)
	}

	command = strings.Replace(command, "{lostSubordinates}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{lostReplicas}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{subordinateHosts}", analysisEntry.SubordinateHosts.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{replicaHosts}", analysisEntry.SubordinateHosts.ToCommaDelimitedList(), -1)

	return command
}

// applyEnvironmentVariables sets the relevant environment variables for a recovery
func applyEnvironmentVariables(topologyRecovery *TopologyRecovery) []string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	env := goos.Environ()
	env = append(env, fmt.Sprintf("ORC_FAILURE_TYPE=%s", string(analysisEntry.Analysis)))
	env = append(env, fmt.Sprintf("ORC_FAILURE_DESCRIPTION=%s", analysisEntry.Description))
	env = append(env, fmt.Sprintf("ORC_COMMAND=%s", analysisEntry.CommandHint))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", analysisEntry.AnalyzedInstanceKey.Hostname))
	env = append(env, fmt.Sprintf("ORC_FAILED_PORT=%d", analysisEntry.AnalyzedInstanceKey.Port))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER=%s", analysisEntry.ClusterDetails.ClusterName))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_ALIAS=%s", analysisEntry.ClusterDetails.ClusterAlias))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_DOMAIN=%s", analysisEntry.ClusterDetails.ClusterDomain))
	env = append(env, fmt.Sprintf("ORC_COUNT_REPLICAS=%d", analysisEntry.CountReplicas))
	env = append(env, fmt.Sprintf("ORC_IS_DOWNTIMED=%v", analysisEntry.IsDowntimed))
	env = append(env, fmt.Sprintf("ORC_AUTO_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedMainRecovery))
	env = append(env, fmt.Sprintf("ORC_AUTO_INTERMEDIATE_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedIntermediateMainRecovery))
	env = append(env, fmt.Sprintf("ORC_ORCHESTRATOR_HOST=%s", process.ThisHostname))
	env = append(env, fmt.Sprintf("ORC_IS_SUCCESSFUL=%v", (topologyRecovery.SuccessorKey != nil)))
	env = append(env, fmt.Sprintf("ORC_LOST_REPLICAS=%s", topologyRecovery.LostReplicas.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_REPLICA_HOSTS=%s", analysisEntry.SubordinateHosts.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_RECOVERY_UID=%s", topologyRecovery.UID))

	if topologyRecovery.SuccessorKey != nil {
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_HOST=%s", topologyRecovery.SuccessorKey.Hostname))
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_PORT=%d", topologyRecovery.SuccessorKey.Port))
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_ALIAS=%s", topologyRecovery.SuccessorAlias))
	}

	return env
}

// executeProcesses executes a list of processes
func executeProcesses(processes []string, description string, topologyRecovery *TopologyRecovery, failOnError bool) error {
	if len(processes) == 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("No %s hooks to run", description))
		return nil
	}

	var err error
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %d %s hooks", len(processes), description))
	for i, command := range processes {
		fullDescription := fmt.Sprintf("%s hook %d of %d", description, i+1, len(processes))

		command := replaceCommandPlaceholders(command, topologyRecovery)
		env := applyEnvironmentVariables(topologyRecovery)

		// Log the command to be run and record how long it takes as this may be useful
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %s: %s", fullDescription, command))
		start := time.Now()
		if cmdErr := os.CommandRun(command, env); cmdErr == nil {
			info := fmt.Sprintf("Completed %s in %v",
				fullDescription, time.Since(start))
			AuditTopologyRecovery(topologyRecovery, info)
		} else {
			info := fmt.Sprintf("Execution of %s failed in %v with error: %v",
				fullDescription, time.Since(start), cmdErr)
			AuditTopologyRecovery(topologyRecovery, info)
			log.Errorf(info)
			// FIXME: It would be good to additionally include command execution output to the auditing

			if err == nil {
				// Note first error
				err = cmdErr
			}
			if failOnError {
				AuditTopologyRecovery(
					topologyRecovery,
					fmt.Sprintf("Not running further %s hooks", description))
				return err
			}
		}
	}
	AuditTopologyRecovery(
		topologyRecovery,
		fmt.Sprintf("done running %s hooks", description))
	return err
}

func recoverDeadMainInBinlogServerTopology(topologyRecovery *TopologyRecovery) (promotedReplica *inst.Instance, err error) {
	failedMainKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *inst.Instance

	_, promotedBinlogServer, err = inst.RegroupReplicasBinlogServers(failedMainKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	promotedBinlogServer, err = inst.StopSubordinate(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Find candidate replica
	promotedReplica, err = inst.GetCandidateReplicaOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Align it with binlog server coordinates
	promotedReplica, err = inst.StopSubordinate(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StartSubordinateUntilMainCoordinates(&promotedReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StopSubordinate(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Detach, flush binary logs forward
	promotedReplica, err = inst.ResetSubordinate(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogsTo(&promotedReplica.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogs(&promotedReplica.Key, 1)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.PurgeBinaryLogsToLatest(&promotedReplica.Key, false)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Reconnect binlog servers to promoted replica (now main):
	promotedBinlogServer, err = inst.SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedBinlogServer, err = inst.Repoint(&promotedBinlogServer.Key, &promotedReplica.Key, inst.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		// Move binlog server replicas up to replicate from main.
		// This can only be done once a BLS has skipped to the next binlog
		// We postpone this operation. The main is already promoted and we're happy.
		binlogServerReplicas, err := inst.ReadBinlogServerReplicaInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerReplica := range binlogServerReplicas {
			binlogServerReplica := binlogServerReplica
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerReplica, err := inst.StopSubordinate(&binlogServerReplica.Key)
				if err != nil {
					return err
				}
				// Make sure the BLS has the "next binlog" -- the one the main flushed & purged to. Otherwise the BLS
				// will request a binlog the main does not have
				if binlogServerReplica.ExecBinlogCoordinates.SmallerThan(&promotedBinlogServer.ExecBinlogCoordinates) {
					binlogServerReplica, err = inst.StartSubordinateUntilMainCoordinates(&binlogServerReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = inst.Repoint(&binlogServerReplica.Key, &promotedReplica.Key, inst.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("recoverDeadMainInBinlogServerTopology, moving binlog server %+v", binlogServerReplica.Key))
		}
	}()

	return promotedReplica, err
}

// recoverDeadMain recovers a dead main, complete logic inside
func recoverDeadMain(topologyRecovery *TopologyRecovery, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = MainRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	var cannotReplicateReplicas [](*inst.Instance)
	postponedAll := false

	inst.AuditOperation("recover-dead-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: will recover %+v", *failedInstanceKey))

	var mainRecoveryType MainRecoveryType = MainRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		mainRecoveryType = MainRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		mainRecoveryType = MainRecoveryBinlogServer
	}
	topologyRecovery.RecoveryType = mainRecoveryType
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: mainRecoveryType=%+v", mainRecoveryType))

	promotedReplicaIsIdeal := func(promoted *inst.Instance) bool {
		if promoted == nil {
			return false
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: promotedReplicaIsIdeal(%+v)", promoted.Key))
		if promoted.Key.Equals(candidateInstanceKey) {
			return true
		}
		if candidateInstanceKey == nil {
			if promoted.PromotionRule == inst.MustPromoteRule || promoted.PromotionRule == inst.PreferPromoteRule {
				if promoted.DataCenter == topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter &&
					promoted.PhysicalEnvironment == topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: found %+v to be ideal candidate; will optimize recovery", promoted.Key))
					postponedAll = true
					return true
				}
			}
		}
		return false
	}
	switch mainRecoveryType {
	case MainRecoveryGTID:
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: regrouping replicas via GTID"))
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MainRecoveryPseudoGTID:
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: regrouping replicas via Pseudo-GTID"))
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MainRecoveryBinlogServer:
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: recovering via binlog servers"))
			promotedReplica, err = recoverDeadMainInBinlogServerTopology(topologyRecovery)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	for _, replica := range lostReplicas {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMainFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMainHost(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMain, detach %+v lost replicas", len(lostReplicas)))
	}

	func() error {
		inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))

	if promotedReplica != nil && !postponedAll {
		promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, &analysisEntry.AnalyzedInstanceKey, promotedReplica, candidateInstanceKey)
		topologyRecovery.AddError(err)
	}

	if promotedReplica == nil {
		message := "Failure: no replica promoted."
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-main", failedInstanceKey, message)
	} else {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-main", failedInstanceKey, message)
	}
	return promotedReplica, lostReplicas, err
}

func CrossDataCenterMainFailoverConstraintSatisfied(analysisEntry *inst.ReplicationAnalysis, suggestedInstance *inst.Instance) bool {
	if !config.Config.PreventCrossDataCenterMainFailover {
		// There is no constraint. We are satisfied
		return true
	}
	if suggestedInstance.DataCenter == analysisEntry.AnalyzedInstanceDataCenter {
		return true
	}
	return false
}

// SuggestReplacementForPromotedReplica returns a server to take over the already
// promoted replica, if such server is found and makes an improvement over the promoted replica.
func SuggestReplacementForPromotedReplica(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (replacement *inst.Instance, actionRequired bool, err error) {
	candidateReplicas, _ := inst.ReadClusterCandidateInstances(promotedReplica.ClusterName)
	candidateReplicas = inst.RemoveInstance(candidateReplicas, deadInstanceKey)
	deadInstance, _, err := inst.ReadInstance(deadInstanceKey)
	if err != nil {
		deadInstance = nil
	}
	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the main
	// There's many options. We may wish to replace the server we promoted with a better one.
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("checking if should replace promoted replica with a better candidate"))
	if candidateInstanceKey == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is the ideal candidate"))
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if promotedReplica.Key.Equals(&candidateReplica.Key) &&
					promotedReplica.DataCenter == deadInstance.DataCenter &&
					promotedReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					// Seems like we promoted a candidate in the same DC & ENV as dead IM! Ideal! We're happy!
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is the ideal candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				}
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the dead instance
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for an ideal candidate"))
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if canTakeOverPromotedServerAsMain(candidateReplica, promotedReplica) &&
					candidateReplica.DataCenter == deadInstance.DataCenter &&
					candidateReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					// This would make a great candidate
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as failed instance", *deadInstanceKey, candidateReplica.Key))
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		// We cannot find a candidate in same DC and ENV as dead main
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is an OK candidate"))
		for _, candidateReplica := range candidateReplicas {
			if promotedReplica.Key.Equals(&candidateReplica.Key) {
				// Seems like we promoted a candidate replica (though not in same DC and ENV as dead main)
				if CrossDataCenterMainFailoverConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica) {
					// Good enough. No further action required.
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v because PreventCrossDataCenterMainFailover is set", candidateReplica.Key))
				}
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMain(candidateReplica, promotedReplica) &&
				promotedReplica.DataCenter == candidateReplica.DataCenter &&
				promotedReplica.PhysicalEnvironment == candidateReplica.PhysicalEnvironment {
				// OK, better than nothing
				candidateInstanceKey = &candidateReplica.Key
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, candidateReplica.Key))
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMain(candidateReplica, promotedReplica) {
				if CrossDataCenterMainFailoverConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica) {
					// OK, better than nothing
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement", promotedReplica.Key, candidateReplica.Key))
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v because PreventCrossDataCenterMainFailover is set", candidateReplica.Key))
				}
			}
		}
	}

	keepSearchingHint := ""
	if !CrossDataCenterMainFailoverConstraintSatisfied(&topologyRecovery.AnalysisEntry, promotedReplica) {
		keepSearchingHint = fmt.Sprintf("Will keep searching because PreventCrossDataCenterMainFailover is set and we haven't promoted a server in %s data center", topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter)
	} else if promotedReplica.PromotionRule == inst.PreferNotPromoteRule {
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", promotedReplica.Key)
	}
	if keepSearchingHint != "" {
		AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		neutralReplicas, _ := inst.ReadClusterNeutralPromotionRuleInstances(promotedReplica.ClusterName)

		if candidateInstanceKey == nil {
			// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
			// find neutral instance in same dv&env as dead main
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as dead main"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMain(neutralReplica, promotedReplica) &&
					deadInstance.DataCenter == neutralReplica.DataCenter &&
					deadInstance.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as dead main", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			// find neutral instance in same dv&env as promoted replica
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMain(neutralReplica, promotedReplica) &&
					promotedReplica.DataCenter == neutralReplica.DataCenter &&
					promotedReplica.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace a prefer_not"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMain(neutralReplica, promotedReplica) {
					if CrossDataCenterMainFailoverConstraintSatisfied(&topologyRecovery.AnalysisEntry, neutralReplica) {
						// OK, better than nothing
						candidateInstanceKey = &neutralReplica.Key
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on promoted instance having prefer_not promotion rule", promotedReplica.Key, neutralReplica.Key))
					} else {
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v because PreventCrossDataCenterMainFailover is set", neutralReplica.Key))
					}
				}
			}
		}
	}

	// So do we have a candidate?
	if candidateInstanceKey == nil {
		// Found nothing. Stick with promoted replica
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ found no server to promote on top promoted replica"))
		return promotedReplica, false, nil
	}
	if promotedReplica.Key.Equals(candidateInstanceKey) {
		// Sanity. It IS the candidate, nothing to promote...
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ sanity check: found our very own server to promote; doing nothing"))
		return promotedReplica, false, nil
	}
	replacement, _, err = inst.ReadInstance(candidateInstanceKey)
	return replacement, true, err
}

// replacePromotedReplicaWithCandidate is called after a main (or co-main)
// died and was replaced by some promotedReplica.
// But, is there an even better replica to promote?
// if candidateInstanceKey is given, then it is forced to be promoted over the promotedReplica
// Otherwise, search for the best to promote!
func replacePromotedReplicaWithCandidate(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (*inst.Instance, error) {
	candidateInstance, actionRequired, err := SuggestReplacementForPromotedReplica(topologyRecovery, deadInstanceKey, promotedReplica, candidateInstanceKey)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	if !actionRequired {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v requires no further action", promotedReplica.Key))
		return promotedReplica, nil
	}

	// Try and promote suggested candidate, if applicable and possible
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedReplica.Key, candidateInstance.Key))

	if candidateInstance.MainKey.Equals(&promotedReplica.Key) {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: suggested candidate %+v is replica of promoted instance %+v. Will try and take its main", candidateInstance.Key, promotedReplica.Key))
		candidateInstance, err = inst.TakeMain(&candidateInstance.Key, topologyRecovery.Type == CoMainRecovery)
		if err != nil {
			return promotedReplica, log.Errore(err)
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("success promoting %+v over %+v", candidateInstance.Key, promotedReplica.Key))

		// As followup to taking over, let's relocate all the rest of the replicas under the candidate instance
		relocateReplicasFunc := func() error {
			log.Debugf("replace-promoted-replica-with-candidate: relocating replicas of %+v below %+v", promotedReplica.Key, candidateInstance.Key)

			relocatedReplicas, _, err, _ := inst.RelocateReplicas(&promotedReplica.Key, &candidateInstance.Key, "")
			log.Debugf("replace-promoted-replica-with-candidate: + relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key))
			return log.Errore(err)
		}
		postponedFunctionsContainer := &topologyRecovery.PostponedFunctionsContainer
		if postponedFunctionsContainer != nil {
			postponedFunctionsContainer.AddPostponedFunction(relocateReplicasFunc, fmt.Sprintf("replace-promoted-replica-with-candidate: relocate replicas of %+v", promotedReplica.Key))
		} else {
			_ = relocateReplicasFunc()
			// We do not propagate the error. It is logged, but otherwise should not fail the entire failover operation
		}
		return candidateInstance, nil
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not manage to promoted suggested candidate %+v", candidateInstance.Key))
	return promotedReplica, nil
}

// checkAndRecoverDeadMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadMain.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("will handle DeadMain event on %+v", analysisEntry.ClusterDetails.ClusterName))
	recoverDeadMainCounter.Inc(1)
	promotedReplica, lostReplicas, err := recoverDeadMain(topologyRecovery, candidateInstanceKey, skipProcesses)
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)

	// And this is the end; whether successful or not, we're done.
	resolveRecovery(topologyRecovery, promotedReplica)
	if promotedReplica != nil {
		if config.Config.FailMainPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			message := fmt.Sprintf("RecoverDeadMain: failed promotion. FailMainPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
			AuditTopologyRecovery(topologyRecovery, message)
			return false, nil, log.Error(message)
		}
		if !CrossDataCenterMainFailoverConstraintSatisfied(&analysisEntry, promotedReplica) {
			message := fmt.Sprintf("RecoverDeadMain: failed promotion. PreventCrossDataCenterMainFailover is set, and promoted replica %+v is in %s while dead main was in %s.", promotedReplica.Key, promotedReplica.DataCenter, analysisEntry.AnalyzedInstanceDataCenter)
			AuditTopologyRecovery(topologyRecovery, message)
			return false, nil, log.Error(message)
		}
		if config.Config.DelayMainPromotionIfSQLThreadNotUpToDate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting to ensure the SQL thread catches up on %+v", promotedReplica.Key))
			if _, err = inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return false, nil, err
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("SQL thread caught up on %+v", promotedReplica.Key))
		}

		// Success!
		recoverDeadMainSuccessCounter.Inc(1)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMain: successfully promoted %+v", promotedReplica.Key))
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: promoted server coordinates: %+v", promotedReplica.SelfBinlogCoordinates))

		if config.Config.ApplyMySQLPromotionAfterMainFailover || analysisEntry.CommandHint == inst.GracefulMainTakeoverCommandHint {
			// on GracefulMainTakeoverCommandHint it makes utter sense to RESET SLAVE ALL and read_only=0, and there is no sense in not doing so.
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: will apply MySQL changes to promoted main"))
			{
				_, err := inst.ResetSubordinateOperation(&promotedReplica.Key)
				if err != nil {
					// Ugly, but this is important. Let's give it another try
					_, err = inst.ResetSubordinateOperation(&promotedReplica.Key)
				}
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: applying RESET SLAVE ALL on promoted main: success=%t", (err == nil)))
				if err != nil {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: NOTE that %+v is promoted even though SHOW SLAVE STATUS may still show it has a main", promotedReplica.Key))
				}
			}
			{
				_, err := inst.SetReadOnly(&promotedReplica.Key, false)
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: applying read-only=0 on promoted main: success=%t", (err == nil)))
			}
			// Let's attempt, though we won't necessarily succeed, to set old main as read-only
			go func() {
				_, err := inst.SetReadOnly(&analysisEntry.AnalyzedInstanceKey, true)
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: applying read-only=1 on demoted main: success=%t", (err == nil)))
			}()
		}

		kvPairs := inst.GetClusterMainKVPairs(analysisEntry.ClusterDetails.ClusterAlias, &promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		if orcraft.IsRaftEnabled() {
			for _, kvPair := range kvPairs {
				_, err := orcraft.PublishCommand("put-key-value", kvPair)
				log.Errore(err)
			}
			// since we'll be affecting 3rd party tools here, we _prefer_ to mitigate re-applying
			// of the put-key-value event upon startup. We _recommend_ a snapshot in the near future.
			go orcraft.PublishCommand("async-snapshot", "")
		} else {
			for _, kvPair := range kvPairs {
				err := kv.PutKVPair(kvPair)
				log.Errore(err)
			}
		}
		{
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if !skipProcesses {
			// Execute post main-failover processes
			executeProcesses(config.Config.PostMainFailoverProcesses, "PostMainFailoverProcesses", topologyRecovery, false)
		}

		if config.Config.MainFailoverDetachReplicaMainHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: detaching main host on promoted main"))
				inst.DetachReplicaMainHost(&promotedReplica.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMain, detaching promoted main host %+v", promotedReplica.Key))
		}
		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: updating cluster_alias: %v -> %v", before, after))
			if alias := analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())
	} else {
		recoverDeadMainFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

// isGeneralyValidAsCandidateSiblingOfIntermediateMain sees that basic server configuration and state are valid
func isGeneralyValidAsCandidateSiblingOfIntermediateMain(sibling *inst.Instance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogSubordinateUpdatesEnabled {
		return false
	}
	if !sibling.ReplicaRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMain checks to see that the given sibling is capable to take over instance's replicas
func isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance *inst.Instance, sibling *inst.Instance) bool {
	if sibling.Key.Equals(&intermediateMainInstance.Key) {
		// same instance
		return false
	}
	if !isGeneralyValidAsCandidateSiblingOfIntermediateMain(sibling) {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMainInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsBinlogServer() != intermediateMainInstance.IsBinlogServer() {
		// When both are binlog servers, failover is trivial.
		// When failed IM is binlog server, its sibling is still valid, but we catually prefer to just repoint the replica up -- simplest!
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMainInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

func isGenerallyValidAsWouldBeMain(replica *inst.Instance, requireLogSubordinateUpdates bool) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if requireLogSubordinateUpdates && !replica.LogSubordinateUpdatesEnabled {
		return false
	}
	if replica.IsBinlogServer() {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(replica) {
		return false
	}

	return true
}

func canTakeOverPromotedServerAsMain(wantToTakeOver *inst.Instance, toBeTakenOver *inst.Instance) bool {
	if !isGenerallyValidAsWouldBeMain(wantToTakeOver, true) {
		return false
	}
	if !wantToTakeOver.MainKey.Equals(&toBeTakenOver.Key) {
		return false
	}
	if canReplicate, _ := toBeTakenOver.CanReplicateFrom(wantToTakeOver); !canReplicate {
		return false
	}
	return true
}

// GetCandidateSiblingOfIntermediateMain chooses the best sibling of a dead intermediate main
// to whom the IM's replicas can be moved.
func GetCandidateSiblingOfIntermediateMain(topologyRecovery *TopologyRecovery, intermediateMainInstance *inst.Instance) (*inst.Instance, error) {

	siblings, err := inst.ReadReplicaInstances(&intermediateMainInstance.MainKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMainInstance.Key)
	}

	sort.Sort(sort.Reverse(InstancesByCountReplicas(siblings)))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("searching for the best candidate sibling of dead intermediate main %+v", intermediateMainInstance.Key))
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMainInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMainInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as the ideal candidate", sibling.Key))
			return sibling, nil
		}
	}
	// No candidate in same DC & env, let's search for a candidate anywhere
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) && sibling.IsCandidate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [candidate sibling]", sibling.Key, intermediateMainInstance.Key))
			return sibling, nil
		}
	}
	// Go for some valid in the same DC & ENV
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) &&
			sibling.DataCenter == intermediateMainInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMainInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [same dc & environment]", sibling.Key, intermediateMainInstance.Key))
			return sibling, nil
		}
	}
	// Just whatever is valid.
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [any sibling]", sibling.Key, intermediateMainInstance.Key))
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMainInstance.Key)
}

// RecoverDeadIntermediateMain performs intermediate main recovery; complete logic inside
func RecoverDeadIntermediateMain(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	topologyRecovery.Type = IntermediateMainRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMainInstance, _, err := inst.ReadInstance(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find possible candidate
	candidateSiblingOfIntermediateMain, _ := GetCandidateSiblingOfIntermediateMain(topologyRecovery, intermediateMainInstance)
	relocateReplicasToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMain == nil {
			return
		}
		// We have a candidate
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: will attempt a candidate intermediate main: %+v", candidateSiblingOfIntermediateMain.Key))
		relocatedReplicas, candidateSibling, err, errs := inst.RelocateReplicas(failedInstanceKey, &candidateSiblingOfIntermediateMain.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMain.Key)

		if len(relocatedReplicas) == 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: failed to move any replica to candidate intermediate main (%+v)", candidateSibling.Key))
			return
		}
		if err != nil || len(errs) > 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: move to candidate intermediate main (%+v) did not complete: err: %+v, errs: %+v", candidateSibling.Key, err, errs))
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, fmt.Sprintf("Relocated %d replicas under candidate sibling: %+v; %d errors: %+v", len(relocatedReplicas), candidateSibling.Key, len(errs), errs))
		}
	}
	// Plan A: find a replacement intermediate main in same Data Center
	if candidateSiblingOfIntermediateMain != nil && candidateSiblingOfIntermediateMain.DataCenter == intermediateMainInstance.DataCenter {
		relocateReplicasToCandidateSibling()
	}
	if !recoveryResolved {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: will next attempt regrouping of replicas"))
		// Plan B: regroup (we wish to reduce cross-DC replication streams)
		lostReplicas, _, _, _, regroupPromotedReplica, regroupError := inst.RegroupReplicas(failedInstanceKey, true, nil, nil)
		if regroupError != nil {
			topologyRecovery.AddError(regroupError)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: regroup failed on: %+v", regroupError))
		}
		if regroupPromotedReplica != nil {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: regrouped under %+v, with %d lost replicas", regroupPromotedReplica.Key, len(lostReplicas)))
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedReplica.Key)
			if len(lostReplicas) == 0 && regroupError == nil {
				// Seems like the regroup worked flawlessly. The local replica took over all of its siblings.
				// We can consider this host to be the successor.
				successorInstance = regroupPromotedReplica
			}
		}
		// Plan C: try replacement intermediate main in other DC...
		if candidateSiblingOfIntermediateMain != nil && candidateSiblingOfIntermediateMain.DataCenter != intermediateMainInstance.DataCenter {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: will next attempt relocating to another DC server"))
			relocateReplicasToCandidateSibling()
		}
	}
	if !recoveryResolved {
		// Do we still have leftovers? some replicas couldn't move? Couldn't regroup? Only left with regroup's resulting leader?
		// nothing moved?
		// We don't care much if regroup made it or not. We prefer that it made it, in which case we only need to relocate up
		// one replica, but the operation is still valid if regroup partially/completely failed. We just promote anything
		// not regrouped.
		// So, match up all that's left, plan D
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: will next attempt to relocate up from %+v", *failedInstanceKey))

		relocatedReplicas, mainInstance, err, errs := inst.RelocateReplicas(failedInstanceKey, &analysisEntry.AnalyzedInstanceMainKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceMainKey)

		if len(relocatedReplicas) > 0 {
			recoveryResolved = true
			if successorInstance == nil {
				// There could have been a local replica taking over its siblings. We'd like to consider that one as successor.
				successorInstance = mainInstance
			}
			inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, fmt.Sprintf("Relocated replicas under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMain failed to match up any replica from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	resolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadIntermediateMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadIntermediateMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMain: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMain.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadIntermediateMainCounter.Inc(1)
	promotedReplica, err := RecoverDeadIntermediateMain(topologyRecovery, skipProcesses)
	if promotedReplica != nil {
		// success
		recoverDeadIntermediateMainSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post intermediate-main-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostIntermediateMainFailoverProcesses, "PostIntermediateMainFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMainFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadCoMain recovers a dead co-main, complete logic inside
func RecoverDeadCoMain(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = CoMainRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMainKey := &analysisEntry.AnalyzedInstanceMainKey
	otherCoMain, found, _ := inst.ReadInstance(otherCoMainKey)
	if otherCoMain == nil || !found {
		return nil, lostReplicas, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMain: could not read info for co-main %+v of %+v", *otherCoMainKey, *failedInstanceKey))
	}
	inst.AuditOperation("recover-dead-co-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMain: will recover %+v", *failedInstanceKey))

	var coMainRecoveryType MainRecoveryType = MainRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMainRecoveryType = MainRecoveryGTID
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMain: coMainRecoveryType=%+v", coMainRecoveryType))

	var cannotReplicateReplicas [](*inst.Instance)
	switch coMainRecoveryType {
	case MainRecoveryGTID:
		{
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	case MainRecoveryPseudoGTID:
		{
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	mustPromoteOtherCoMain := config.Config.CoMainRecoveryMustPromoteOtherCoMain
	if !otherCoMain.ReadOnly {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMain: other co-main %+v is writeable hence has to be promoted", otherCoMain.Key))
		mustPromoteOtherCoMain = true
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMain: mustPromoteOtherCoMain? %+v", mustPromoteOtherCoMain))

	if promotedReplica != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
		if mustPromoteOtherCoMain {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMain: mustPromoteOtherCoMain. Verifying that %+v is/can be promoted", *otherCoMainKey))
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, otherCoMainKey)
		} else {
			// We are allowed to promote any server
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, nil)
		}
		topologyRecovery.AddError(err)
	}
	if promotedReplica != nil {
		if mustPromoteOtherCoMain && !promotedReplica.Key.Equals(otherCoMainKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMain: could not manage to promote other-co-main %+v; was only able to promote %+v; CoMainRecoveryMustPromoteOtherCoMain is true, therefore failing", *otherCoMainKey, promotedReplica.Key))
			promotedReplica = nil
		}
	}
	if promotedReplica != nil {
		if config.Config.DelayMainPromotionIfSQLThreadNotUpToDate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting to ensure the SQL thread catches up on %+v", promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return promotedReplica, lostReplicas, err
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("SQL thread caught up on %+v", promotedReplica.Key))
		}
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
	}

	// OK, we may have someone promoted. Either this was the other co-main or another replica.
	// Noting down that we DO NOT attempt to set a new co-main topology. We are good with remaining with a single main.
	// I tried solving the "let's promote a replica and create a new co-main setup" but this turns so complex due to various factors.
	// I see this as risky and not worth the questionable benefit.
	// Maybe future me is a smarter person and finds a simple solution. Unlikely. I'm getting dumber.
	//
	// ...
	// Now that we're convinved, take a look at what we can be left with:
	// Say we started with M1<->M2<-S1, with M2 failing, and we promoted S1.
	// We now have M1->S1 (because S1 is promoted), S1->M2 (because that's what it remembers), M2->M1 (because that's what it remembers)
	// !! This is an evil 3-node circle that must be broken.
	// config.Config.ApplyMySQLPromotionAfterMainFailover, if true, will cause it to break, because we would RESET SLAVE on S1
	// but we want to make sure the circle is broken no matter what.
	// So in the case we promoted not-the-other-co-main, we issue a detach-replica-main-host, which is a reversible operation
	if promotedReplica != nil && !promotedReplica.Key.Equals(otherCoMainKey) {
		_, err = inst.DetachReplicaMainHost(&promotedReplica.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMainFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadCoMain: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMainHost(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadCoMain, detaching %+v replicas", len(lostReplicas)))
	}

	func() error {
		inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	return promotedReplica, lostReplicas, err
}

// checkAndRecoverDeadCoMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadCoMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMain.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadCoMainCounter.Inc(1)
	promotedReplica, lostReplicas, err := RecoverDeadCoMain(topologyRecovery, skipProcesses)
	resolveRecovery(topologyRecovery, promotedReplica)
	if promotedReplica == nil {
		inst.AuditOperation("recover-dead-co-main", failedInstanceKey, "Failure: no replica promoted.")
	} else {
		inst.AuditOperation("recover-dead-co-main", failedInstanceKey, fmt.Sprintf("promoted: %+v", promotedReplica.Key))
	}
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	if promotedReplica != nil {
		if config.Config.FailMainPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return false, nil, log.Errorf("Promoted replica %+v: sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		// success
		recoverDeadCoMainSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMainFailover {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMain: will apply MySQL changes to promoted main"))
			inst.SetReadOnly(&promotedReplica.Key, false)
		}
		if !skipProcesses {
			// Execute post intermediate-main-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostMainFailoverProcesses, "PostMainFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMainFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return
	}
	go inst.ExecuteOnTopology(func() {
		inst.ReadTopologyInstance(instanceKey)
		inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	})
}

// Force reading of replicas of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its replicas.
func emergentlyReadTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyReadTopologyInstance(&replica.Key, analysisCode)
	}
}

// emergentlyRestartReplicationOnTopologyInstance forces a RestartSubordinate on a given instance.
func emergentlyRestartReplicationOnTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted on this specific replica
		return
	}
	go inst.ExecuteOnTopology(func() {
		inst.RestartIOThread(instanceKey)
		inst.AuditOperation("emergently-restart-replication-topology-instance", instanceKey, string(analysisCode))
	})
}

func beginEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) {
	emergencyOperationGracefulPeriodMap.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
}

func isInEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) bool {
	_, found := emergencyOperationGracefulPeriodMap.Get(instanceKey.StringCode())
	return found
}

// emergentlyRestartReplicationOnTopologyInstanceReplicas forces a stop subordinate + start subordinate on
// replicas of a given instance, in an attempt to cause them to re-evaluate their replication state.
// This can be useful in scenarios where the main has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the main is bad.
func emergentlyRestartReplicationOnTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// While each replica's RestartSubordinate() is throttled on its own, it's also wasteful to
		// iterate all replicas all the time. This is the reason why we do grand-throttle check.
		return
	}
	beginEmergencyOperationGracefulPeriod(instanceKey)

	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyRestartReplicationOnTopologyInstance(&replica.Key, analysisCode)
	}
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes.
func checkAndExecuteFailureDetectionProcesses(analysisEntry inst.ReplicationAnalysis, skipProcesses bool) (detectionRegistrationSuccess bool, processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		if util.ClearToLog("checkAndExecuteFailureDetectionProcesses", analysisEntry.AnalyzedInstanceKey.StringCode()) {
			log.Infof("checkAndExecuteFailureDetectionProcesses: could not register %+v detection on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
		}
		return false, false, nil
	}
	log.Infof("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// Execute on-detection processes
	if skipProcesses {
		return true, false, nil
	}
	err = executeProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", NewTopologyRecovery(analysisEntry), true)
	return true, true, err
}

func getCheckAndRecoverFunction(analysisCode inst.AnalysisCode, analyzedInstanceKey *inst.InstanceKey) (
	checkAndRecoverFunction func(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
	isActionableRecovery bool,
) {
	switch analysisCode {
	// main
	case inst.DeadMain, inst.DeadMainAndSomeSubordinates:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverDeadMain, true
		}
	// intermediate main
	case inst.DeadIntermediateMain:
		return checkAndRecoverDeadIntermediateMain, true
	case inst.DeadIntermediateMainAndSomeSubordinates:
		return checkAndRecoverDeadIntermediateMain, true
	case inst.DeadIntermediateMainWithSingleSubordinateFailingToConnect:
		return checkAndRecoverDeadIntermediateMain, true
	case inst.AllIntermediateMainSubordinatesFailingToConnectOrDead:
		return checkAndRecoverDeadIntermediateMain, true
	case inst.DeadIntermediateMainAndSubordinates:
		return checkAndRecoverGenericProblem, false
	// co-main
	case inst.DeadCoMain:
		return checkAndRecoverDeadCoMain, true
	case inst.DeadCoMainAndSomeSubordinates:
		return checkAndRecoverDeadCoMain, true
	// main, non actionable
	case inst.DeadMainAndSubordinates:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMain:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMainWithLaggingReplicas:
		return checkAndRecoverGenericProblem, false
	case inst.AllMainSubordinatesNotReplicating:
		return checkAndRecoverGenericProblem, false
	case inst.AllMainSubordinatesNotReplicatingOrDead:
		return checkAndRecoverGenericProblem, false
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllMainSubordinatesStale:
	//   return checkAndRecoverGenericProblem, false

	return nil, false
}

func runEmergentOperations(analysisEntry *inst.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case inst.DeadMainAndSubordinates:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMainKey, analysisEntry.Analysis)
	case inst.UnreachableMain:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.UnreachableMainWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMainSubordinatesNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMainSubordinatesNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.FirstTierSubordinateFailingToConnectToMain:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMainKey, analysisEntry.Analysis)
	}
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	atomic.AddInt64(&countPendingRecoveries, 1)
	defer atomic.AddInt64(&countPendingRecoveries, -1)

	checkAndRecoverFunction, isActionableRecovery := getCheckAndRecoverFunction(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(&analysisEntry)

	if checkAndRecoverFunction == nil {
		// Unhandled problem type
		if analysisEntry.Analysis != inst.NoProblem {
			if util.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceKey.StringCode()) {
				log.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: %+v; key: %+v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
			}
		}

		return false, nil, nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.

	if orcraft.IsRaftEnabled() {
		// with raft, all nodes can (and should) run analysis,
		// but only the leader proceeds to execute detection hooks and then to failover.
		if !orcraft.IsLeader() {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT detecting/recovering host (raft non-leader)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
			return false, nil, err
		}
	}

	// Initiate detection:
	registrationSuccess, _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses)
	if registrationSuccess {
		if orcraft.IsRaftEnabled() {
			_, err := orcraft.PublishCommand("register-failure-detection", analysisEntry)
			log.Errore(err)
		}
	}
	if err != nil {
		log.Errorf("executeCheckAndRecoverFunction: error on failure detection: %+v", err)
		return false, nil, err
	}
	// We don't mind whether detection really executed the processes or not
	// (it may have been silenced due to previous detection). We only care there's no error.

	// We're about to embark on recovery shortly...

	// Check for recovery being disabled globally
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this
		log.Errorf("Unable to determine if recovery is disabled globally: %v", err)
	} else if recoveryDisabledGlobally {
		if !forceInstanceRecovery {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT Recovering host (disabled globally)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)

			return false, nil, err
		}
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: recoveries disabled globally but forcing this recovery",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
	}

	// Actually attempt recovery:
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}
	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", *topologyRecovery)
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			// Execute general unsuccessful post failover processes
			executeProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			// Execute general post failover processes
			inst.EndDowntime(topologyRecovery.SuccessorKey)
			executeProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
	}
	return recoveryAttempted, topologyRecovery, err
}

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *inst.InstanceKey, err error) {
	// Allow the analysis to run even if we don't want to recover
	replicationAnalysis, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{IncludeDowntimed: true, AuditAnalysis: true})
	if err != nil {
		return false, nil, log.Errore(err)
	}
	if *config.RuntimeCLIFlags.Noop {
		log.Infof("--noop provided; will not execute processes")
		skipProcesses = true
	}
	// intentionally iterating entries in random order
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]
		if specificInstance != nil {
			// We are looking for a specific instance; if this is not the one, skip!
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.SkippableDueToDowntime && specificInstance == nil {
			// Only recover a downtimed server if explicitly requested
			continue
		}

		if specificInstance != nil {
			// force mode. Keep it synchronuous
			var topologyRecovery *TopologyRecovery
			recoveryAttempted, topologyRecovery, err = executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
			log.Errore(err)
			if topologyRecovery != nil {
				promotedReplicaKey = topologyRecovery.SuccessorKey
			}
		} else {
			go func() {
				_, _, err := executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
				log.Errore(err)
			}()
		}
	}
	return recoveryAttempted, promotedReplicaKey, err
}

func forceAnalysisEntry(clusterName string, analysisCode inst.AnalysisCode, commandHint string, failedInstanceKey *inst.InstanceKey) (analysisEntry inst.ReplicationAnalysis, err error) {
	clusterInfo, err := inst.ReadClusterInfo(clusterName)
	if err != nil {
		return analysisEntry, err
	}

	clusterAnalysisEntries, err := inst.GetReplicationAnalysis(clusterInfo.ClusterName, &inst.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		return analysisEntry, err
	}

	for _, entry := range clusterAnalysisEntries {
		if entry.AnalyzedInstanceKey.Equals(failedInstanceKey) {
			analysisEntry = entry
		}
	}
	analysisEntry.Analysis = analysisCode // we force this analysis
	analysisEntry.CommandHint = commandHint
	analysisEntry.ClusterDetails = *clusterInfo
	analysisEntry.AnalyzedInstanceKey = *failedInstanceKey

	return analysisEntry, nil
}

// ForceExecuteRecovery can be called to issue a recovery process even if analysis says there is no recovery case.
// The caller of this function injects the type of analysis it wishes the function to assume.
// By calling this function one takes responsibility for one's actions.
func ForceExecuteRecovery(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
}

// ForceMainFailover *trusts* main of given cluster is dead and initiates a failover
func ForceMainFailover(clusterName string) (topologyRecovery *TopologyRecovery, err error) {
	clusterMains, err := inst.ReadClusterWriteableMain(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	if len(clusterMains) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	clusterMain := clusterMains[0]

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMain, inst.ForceMainFailoverCommandHint, &clusterMain.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, nil, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

// ForceMainTakeover *trusts* main of given cluster is dead and fails over to designated instance,
// which has to be its direct child.
func ForceMainTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	clusterMains, err := inst.ReadClusterWriteableMain(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	if len(clusterMains) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	clusterMain := clusterMains[0]

	if !destination.MainKey.Equals(&clusterMain.Key) {
		return nil, fmt.Errorf("You may only promote a direct child of the main %+v. The main of %+v is %+v.", clusterMain.Key, destination.Key, destination.MainKey)
	}
	log.Infof("Will demote %+v and promote %+v instead", clusterMain.Key, destination.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMain, inst.ForceMainTakeoverCommandHint, &clusterMain.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &destination.Key, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

// GracefulMainTakeover will demote main of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the main, then wait
// for the designated replica to catch up with last position.
// It will point old main at the newly promoted main at the correct coordinates, but will not start replication.
func GracefulMainTakeover(clusterName string, designatedKey *inst.InstanceKey) (topologyRecovery *TopologyRecovery, promotedMainCoordinates *inst.BinlogCoordinates, err error) {
	clusterMains, err := inst.ReadClusterMain(clusterName)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot deduce cluster main for %+v; error: %+v", clusterName, err)
	}
	if len(clusterMains) != 1 {
		return nil, nil, fmt.Errorf("Cannot deduce cluster main for %+v. Found %+v potential mains", clusterName, len(clusterMains))
	}
	clusterMain := clusterMains[0]

	clusterMainDirectReplicas, err := inst.ReadReplicaInstances(&clusterMain.Key)
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if len(clusterMainDirectReplicas) == 0 {
		return nil, nil, fmt.Errorf("Main %+v doesn't seem to have replicas", clusterMain.Key)
	}

	var designatedInstance *inst.Instance
	if designatedKey != nil && !designatedKey.IsValid() {
		// An empty or invalid key is as good as no key
		designatedKey = nil
	}
	if designatedKey == nil {
		// Expect a single replica.
		if len(clusterMainDirectReplicas) > 1 {
			return nil, nil, fmt.Errorf("When no target instance indicated, main %+v should only have one replica (making the takeover safe and simple), but has %+v. Aborting", clusterMain.Key, len(clusterMainDirectReplicas))
		}
		designatedInstance = clusterMainDirectReplicas[0]
		log.Infof("GracefulMainTakeover: designated main deduced to be %+v", designatedInstance.Key)
	} else {
		// Verify designated instance is a direct replica of main
		for _, directReplica := range clusterMainDirectReplicas {
			if directReplica.Key.Equals(designatedKey) {
				designatedInstance = directReplica
			}
		}
		if designatedInstance == nil {
			return nil, nil, fmt.Errorf("GracefulMainTakeover: indicated designated instance %+v must be directly replicating from the main %+v", *designatedKey, clusterMain.Key)
		}
		log.Infof("GracefulMainTakeover: designated main instructed to be %+v", designatedInstance.Key)
	}

	if inst.IsBannedFromBeingCandidateReplica(designatedInstance) {
		return nil, nil, fmt.Errorf("GracefulMainTakeover: designated instance %+v cannot be promoted due to promotion rule or it is explicitly ignored in PromotionIgnoreHostnameFilters configuration", designatedInstance.Key)
	}

	mainOfDesignatedInstance, err := inst.GetInstanceMain(designatedInstance)
	if err != nil {
		return nil, nil, err
	}
	if !mainOfDesignatedInstance.Key.Equals(&clusterMain.Key) {
		return nil, nil, fmt.Errorf("Sanity check failure. It seems like the designated instance %+v does not replicate from the main %+v (designated instance's main key is %+v). This error is strange. Panicking", designatedInstance.Key, clusterMain.Key, designatedInstance.MainKey)
	}
	if !designatedInstance.HasReasonableMaintenanceReplicationLag() {
		return nil, nil, fmt.Errorf("Desginated instance %+v seems to be lagging to much for thie operation. Aborting.", designatedInstance.Key)
	}

	if len(clusterMainDirectReplicas) > 1 {
		log.Infof("GracefulMainTakeover: Will let %+v take over its siblings", designatedInstance.Key)
		relocatedReplicas, _, err, _ := inst.RelocateReplicas(&clusterMain.Key, &designatedInstance.Key, "")
		if len(relocatedReplicas) != len(clusterMainDirectReplicas)-1 {
			// We are unable to make designated instance main of all its siblings
			relocatedReplicasKeyMap := inst.NewInstanceKeyMap()
			relocatedReplicasKeyMap.AddInstances(relocatedReplicas)
			// Let's see which replicas have not been relocated
			for _, directReplica := range clusterMainDirectReplicas {
				if relocatedReplicasKeyMap.HasKey(directReplica.Key) {
					// relocated, good
					continue
				}
				if directReplica.Key.Equals(&designatedInstance.Key) {
					// obviously we skip this one
					continue
				}
				if directReplica.IsDowntimed {
					// obviously we skip this one
					log.Warningf("GracefulMainTakeover: unable to relocate %+v below designated %+v, but since it is downtimed (downtime reason: %s) I will proceed", directReplica.Key, designatedInstance.Key, directReplica.DowntimeReason)
					continue
				}
				return nil, nil, fmt.Errorf("Desginated instance %+v cannot take over all of its siblings. Error: %+v", designatedInstance.Key, err)
			}
		}
	}
	log.Infof("GracefulMainTakeover: Will demote %+v and promote %+v instead", clusterMain.Key, designatedInstance.Key)

	replicationUser, replicationPassword, replicationCredentialsError := inst.ReadReplicationCredentials(&designatedInstance.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMain, inst.GracefulMainTakeoverCommandHint, &clusterMain.Key)
	if err != nil {
		return nil, nil, err
	}
	preGracefulTakeoverTopologyRecovery := &TopologyRecovery{
		AnalysisEntry: analysisEntry,
	}
	if err := executeProcesses(config.Config.PreGracefulTakeoverProcesses, "PreGracefulTakeoverProcesses", preGracefulTakeoverTopologyRecovery, true); err != nil {
		return nil, nil, fmt.Errorf("Failed running PreGracefulTakeoverProcesses: %+v", err)
	}

	if designatedInstance, err = inst.StopSubordinate(&designatedInstance.Key); err != nil {
		return nil, nil, err
	}
	log.Infof("GracefulMainTakeover: Will set %+v as read_only", clusterMain.Key)
	if clusterMain, err = inst.SetReadOnly(&clusterMain.Key, true); err != nil {
		return nil, nil, err
	}
	demotedMainSelfBinlogCoordinates := clusterMain.SelfBinlogCoordinates
	log.Infof("GracefulMainTakeover: Will advance %+v to main coordinates %+v", designatedInstance.Key, demotedMainSelfBinlogCoordinates)
	if designatedInstance, err = inst.StartSubordinateUntilMainCoordinates(&designatedInstance.Key, &clusterMain.SelfBinlogCoordinates); err != nil {
		return nil, nil, err
	}
	promotedMainCoordinates = &designatedInstance.SelfBinlogCoordinates

	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &designatedInstance.Key, false)
	if err != nil {
		return nil, nil, err
	}
	if !recoveryAttempted {
		return nil, nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	var gtidHint inst.OperationGTIDHint = inst.GTIDHintNeutral
	if topologyRecovery.RecoveryType == MainRecoveryGTID {
		gtidHint = inst.GTIDHintForce
	}
	clusterMain, err = inst.ChangeMainTo(&clusterMain.Key, &designatedInstance.Key, promotedMainCoordinates, false, gtidHint)
	if !clusterMain.SelfBinlogCoordinates.Equals(&demotedMainSelfBinlogCoordinates) {
		log.Errorf("GracefulMainTakeover: sanity problem. Demoted main's coordinates changed from %+v to %+v while supposed to have been frozen", demotedMainSelfBinlogCoordinates, clusterMain.SelfBinlogCoordinates)
	}
	if !clusterMain.HasReplicationCredentials && replicationCredentialsError == nil {
		_, credentialsErr := inst.ChangeMainCredentials(&clusterMain.Key, replicationUser, replicationPassword)
		if err == nil {
			err = credentialsErr
		}
	}

	if designatedInstance.AllowTLS {
		_, enableSSLErr := inst.EnableMainSSL(&clusterMain.Key)
		if err == nil {
			err = enableSSLErr
		}
	}
	executeProcesses(config.Config.PostGracefulTakeoverProcesses, "PostGracefulTakeoverProcesses", topologyRecovery, false)

	return topologyRecovery, promotedMainCoordinates, err
}

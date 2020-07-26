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

package inst

import (
	"fmt"
	"strings"

	"github.com/github/orchestrator/go/config"
)

type AnalysisCode string
type StructureAnalysisCode string

const (
	NoProblem                                             AnalysisCode = "NoProblem"
	DeadMainWithoutSubordinates                                            = "DeadMainWithoutSubordinates"
	DeadMain                                                         = "DeadMain"
	DeadMainAndSubordinates                                                = "DeadMainAndSubordinates"
	DeadMainAndSomeSubordinates                                            = "DeadMainAndSomeSubordinates"
	UnreachableMainWithLaggingReplicas                               = "UnreachableMainWithLaggingReplicas"
	UnreachableMain                                                  = "UnreachableMain"
	MainSingleSubordinateNotReplicating                                    = "MainSingleSubordinateNotReplicating"
	MainSingleSubordinateDead                                              = "MainSingleSubordinateDead"
	AllMainSubordinatesNotReplicating                                      = "AllMainSubordinatesNotReplicating"
	AllMainSubordinatesNotReplicatingOrDead                                = "AllMainSubordinatesNotReplicatingOrDead"
	MainWithoutSubordinates                                                = "MainWithoutSubordinates"
	DeadCoMain                                                       = "DeadCoMain"
	DeadCoMainAndSomeSubordinates                                          = "DeadCoMainAndSomeSubordinates"
	UnreachableCoMain                                                = "UnreachableCoMain"
	AllCoMainSubordinatesNotReplicating                                    = "AllCoMainSubordinatesNotReplicating"
	DeadIntermediateMain                                             = "DeadIntermediateMain"
	DeadIntermediateMainWithSingleSubordinate                              = "DeadIntermediateMainWithSingleSubordinate"
	DeadIntermediateMainWithSingleSubordinateFailingToConnect              = "DeadIntermediateMainWithSingleSubordinateFailingToConnect"
	DeadIntermediateMainAndSomeSubordinates                                = "DeadIntermediateMainAndSomeSubordinates"
	DeadIntermediateMainAndSubordinates                                    = "DeadIntermediateMainAndSubordinates"
	UnreachableIntermediateMain                                      = "UnreachableIntermediateMain"
	AllIntermediateMainSubordinatesFailingToConnectOrDead                  = "AllIntermediateMainSubordinatesFailingToConnectOrDead"
	AllIntermediateMainSubordinatesNotReplicating                          = "AllIntermediateMainSubordinatesNotReplicating"
	FirstTierSubordinateFailingToConnectToMain                             = "FirstTierSubordinateFailingToConnectToMain"
	BinlogServerFailingToConnectToMain                               = "BinlogServerFailingToConnectToMain"
)

const (
	StatementAndMixedLoggingSubordinatesStructureWarning StructureAnalysisCode = "StatementAndMixedLoggingSubordinatesStructureWarning"
	StatementAndRowLoggingSubordinatesStructureWarning                         = "StatementAndRowLoggingSubordinatesStructureWarning"
	MixedAndRowLoggingSubordinatesStructureWarning                             = "MixedAndRowLoggingSubordinatesStructureWarning"
	MultipleMajorVersionsLoggingSubordinates                                   = "MultipleMajorVersionsLoggingSubordinates"
	DifferentGTIDModesStructureWarning                                   = "DifferentGTIDModesStructureWarning"
	ErrantGTIDStructureWarning                                           = "ErrantGTIDStructureWarning"
)

type InstanceAnalysis struct {
	key      *InstanceKey
	analysis AnalysisCode
}

func NewInstanceAnalysis(instanceKey *InstanceKey, analysis AnalysisCode) *InstanceAnalysis {
	return &InstanceAnalysis{
		key:      instanceKey,
		analysis: analysis,
	}
}

func (instanceAnalysis *InstanceAnalysis) String() string {
	return fmt.Sprintf("%s/%s", instanceAnalysis.key.StringCode(), string(instanceAnalysis.analysis))
}

// PeerAnalysisMap indicates the number of peers agreeing on an analysis.
// Key of this map is a InstanceAnalysis.String()
type PeerAnalysisMap map[string]int

type ReplicationAnalysisHints struct {
	IncludeDowntimed bool
	IncludeNoProblem bool
	AuditAnalysis    bool
}

const (
	ForceMainFailoverCommandHint    string = "force-main-failover"
	ForceMainTakeoverCommandHint    string = "force-main-takeover"
	GracefulMainTakeoverCommandHint string = "graceful-main-takeover"
)

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceKey                       InstanceKey
	AnalyzedInstanceMainKey                 InstanceKey
	ClusterDetails                            ClusterInfo
	AnalyzedInstanceDataCenter                string
	AnalyzedInstancePhysicalEnvironment       string
	IsMain                                  bool
	IsCoMain                                bool
	LastCheckValid                            bool
	LastCheckPartialSuccess                   bool
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountReplicasFailingToConnectToMain     uint
	CountDowntimedReplicas                    uint
	ReplicationDepth                          uint
	SubordinateHosts                                InstanceKeyMap
	IsFailingToConnectToMain                bool
	Analysis                                  AnalysisCode
	Description                               string
	StructureAnalysis                         []StructureAnalysisCode
	IsDowntimed                               bool
	IsReplicasDowntimed                       bool // as good as downtimed because all replicas are downtimed AND analysis is all about the replicas (e.e. AllMainSubordinatesNotReplicating)
	DowntimeEndTimestamp                      string
	DowntimeRemainingSeconds                  int
	IsBinlogServer                            bool
	PseudoGTIDImmediateTopology               bool
	OracleGTIDImmediateTopology               bool
	MariaDBGTIDImmediateTopology              bool
	BinlogServerImmediateTopology             bool
	CountStatementBasedLoggingReplicas        uint
	CountMixedBasedLoggingReplicas            uint
	CountRowBasedLoggingReplicas              uint
	CountDistinctMajorVersionsLoggingReplicas uint
	CountDelayedReplicas                      uint
	CountLaggingReplicas                      uint
	IsActionableRecovery                      bool
	ProcessingNodeHostname                    string
	ProcessingNodeToken                       string
	CountAdditionalAgreeingNodes              int
	StartActivePeriod                         string
	SkippableDueToDowntime                    bool
	GTIDMode                                  string
	MinReplicaGTIDMode                        string
	MaxReplicaGTIDMode                        string
	MaxReplicaGTIDErrant                      string
	CommandHint                               string
}

type AnalysisMap map[string](*ReplicationAnalysis)

type ReplicationAnalysisChangelog struct {
	AnalyzedInstanceKey InstanceKey
	Changelog           []string
}

// ReadReplicaHostsFromString parses and reads replica keys from comma delimited string
func (this *ReplicationAnalysis) ReadReplicaHostsFromString(replicaHostsString string) error {
	this.SubordinateHosts = *NewInstanceKeyMap()
	return this.SubordinateHosts.ReadCommaDelimitedList(replicaHostsString)
}

// AnalysisString returns a human friendly description of all analysis issues
func (this *ReplicationAnalysis) AnalysisString() string {
	result := []string{}
	if this.Analysis != NoProblem {
		result = append(result, string(this.Analysis))
	}
	for _, structureAnalysis := range this.StructureAnalysis {
		result = append(result, string(structureAnalysis))
	}
	return strings.Join(result, ", ")
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.Config.InstancePollSeconds + 1
}

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
	"regexp"
	"time"

	"github.com/github/orchestrator/go/config"
	"github.com/github/orchestrator/go/db"
	"github.com/github/orchestrator/go/process"
	"github.com/github/orchestrator/go/raft"
	"github.com/github/orchestrator/go/util"

	"github.com/openark/golib/log"
	"github.com/openark/golib/sqlutils"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

var recentInstantAnalysis *cache.Cache

func init() {
	metrics.Register("analysis.change.write.attempt", analysisChangeWriteAttemptCounter)
	metrics.Register("analysis.change.write", analysisChangeWriteCounter)

	go initializeAnalysisDaoPostConfiguration()
}

func initializeAnalysisDaoPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	recentInstantAnalysis = cache.New(time.Duration(config.RecoveryPollSeconds*2)*time.Second, time.Second)
}

// GetReplicationAnalysis will check for replication problems (dead main; unreachable main; etc)
func GetReplicationAnalysis(clusterName string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error) {
	result := []ReplicationAnalysis{}

	args := sqlutils.Args(ValidSecondsFromSeenToLastAttemptedCheck(), config.Config.ReasonableReplicationLagSeconds, clusterName)
	analysisQueryReductionClause := ``
	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(MIN(
					main_instance.last_checked <= main_instance.last_seen
					and main_instance.last_attempted_check <= main_instance.last_seen + interval ? second
       	 ) = 1 /* AS is_last_check_valid */) = 0
				OR (IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
		                    AND replica_instance.subordinate_io_running = 0
		                    AND replica_instance.last_io_error like '%error %connecting to main%'
		                    AND replica_instance.subordinate_sql_running = 1),
		                0) /* AS count_replicas_failing_to_connect_to_main */ > 0)
				OR (IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen),
		                0) /* AS count_valid_subordinates */ < COUNT(replica_instance.server_id) /* AS count_replicas */)
				OR (IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
		                    AND replica_instance.subordinate_io_running != 0
		                    AND replica_instance.subordinate_sql_running != 0),
		                0) /* AS count_valid_replicating_subordinates */ < COUNT(replica_instance.server_id) /* AS count_replicas */)
				OR (MIN(
		            main_instance.subordinate_sql_running = 1
		            AND main_instance.subordinate_io_running = 0
		            AND main_instance.last_io_error like '%error %connecting to main%'
		          ) /* AS is_failing_to_connect_to_main */)
				OR (COUNT(replica_instance.server_id) /* AS count_replicas */ > 0)
			`
		args = append(args, ValidSecondsFromSeenToLastAttemptedCheck())
	}
	// "OR count_replicas > 0" above is a recent addition, which, granted, makes some previous conditions redundant.
	// It gives more output, and more "NoProblem" messages that I am now interested in for purpose of auditing in database_instance_analysis_changelog
	query := fmt.Sprintf(`
		    SELECT
		        main_instance.hostname,
		        main_instance.port,
						MIN(main_instance.data_center) AS data_center,
						MIN(main_instance.physical_environment) AS physical_environment,
		        MIN(main_instance.main_host) AS main_host,
		        MIN(main_instance.main_port) AS main_port,
		        MIN(main_instance.cluster_name) AS cluster_name,
		        MIN(IFNULL(cluster_alias.alias, main_instance.cluster_name)) AS cluster_alias,
		        MIN(
							main_instance.last_checked <= main_instance.last_seen
							and main_instance.last_attempted_check <= main_instance.last_seen + interval ? second
		        	) = 1 AS is_last_check_valid,
						MIN(main_instance.last_check_partial_success) as last_check_partial_success,
		        MIN(main_instance.main_host IN ('' , '_')
		            OR main_instance.main_port = 0
								OR substr(main_instance.main_host, 1, 2) = '//') AS is_main,
		        MIN(main_instance.is_co_main) AS is_co_main,
		        MIN(CONCAT(main_instance.hostname,
		                ':',
		                main_instance.port) = main_instance.cluster_name) AS is_cluster_main,
						MIN(main_instance.gtid_mode) AS gtid_mode,
		        COUNT(replica_instance.server_id) AS count_replicas,
		        IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen),
		                0) AS count_valid_subordinates,
		        IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
		                    AND replica_instance.subordinate_io_running != 0
		                    AND replica_instance.subordinate_sql_running != 0),
		                0) AS count_valid_replicating_subordinates,
		        IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
		                    AND replica_instance.subordinate_io_running = 0
		                    AND replica_instance.last_io_error like '%%error %%connecting to main%%'
		                    AND replica_instance.subordinate_sql_running = 1),
		                0) AS count_replicas_failing_to_connect_to_main,
		        MIN(main_instance.replication_depth) AS replication_depth,
		        GROUP_CONCAT(concat(replica_instance.Hostname, ':', replica_instance.Port)) as subordinate_hosts,
		        MIN(
		            main_instance.subordinate_sql_running = 1
		            AND main_instance.subordinate_io_running = 0
		            AND main_instance.last_io_error like '%%error %%connecting to main%%'
		          ) AS is_failing_to_connect_to_main,
						MIN(
								main_downtime.downtime_active is not null
								and ifnull(main_downtime.end_timestamp, now()) > now()
							) AS is_downtimed,
			    	MIN(
				    		IFNULL(main_downtime.end_timestamp, '')
				    	) AS downtime_end_timestamp,
			    	MIN(
				    		IFNULL(unix_timestamp() - unix_timestamp(main_downtime.end_timestamp), 0)
				    	) AS downtime_remaining_seconds,
			    	MIN(
				    		main_instance.binlog_server
				    	) AS is_binlog_server,
			    	MIN(
				    		main_instance.pseudo_gtid
				    	) AS is_pseudo_gtid,
			    	MIN(
				    		main_instance.supports_oracle_gtid
				    	) AS supports_oracle_gtid,
			    	SUM(
				    		replica_instance.oracle_gtid
				    	) AS count_oracle_gtid_subordinates,
			      IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
	              AND replica_instance.oracle_gtid != 0),
              0) AS count_valid_oracle_gtid_subordinates,
			    	SUM(
				    		replica_instance.binlog_server
				    	) AS count_binlog_server_subordinates,
		        IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
                  AND replica_instance.binlog_server != 0),
              0) AS count_valid_binlog_server_subordinates,
			    	MIN(
				    		main_instance.mariadb_gtid
				    	) AS is_mariadb_gtid,
			    	SUM(
				    		replica_instance.mariadb_gtid
				    	) AS count_mariadb_gtid_subordinates,
		        IFNULL(SUM(replica_instance.last_checked <= replica_instance.last_seen
                  AND replica_instance.mariadb_gtid != 0),
              0) AS count_valid_mariadb_gtid_subordinates,
						IFNULL(SUM(replica_instance.log_bin
							  AND replica_instance.log_subordinate_updates
								AND replica_instance.binlog_format = 'STATEMENT'),
              0) AS count_statement_based_loggin_subordinates,
						IFNULL(SUM(replica_instance.log_bin
								AND replica_instance.log_subordinate_updates
								AND replica_instance.binlog_format = 'MIXED'),
              0) AS count_mixed_based_loggin_subordinates,
						IFNULL(SUM(replica_instance.log_bin
								AND replica_instance.log_subordinate_updates
								AND replica_instance.binlog_format = 'ROW'),
              0) AS count_row_based_loggin_subordinates,
						IFNULL(SUM(replica_instance.sql_delay > 0),
              0) AS count_delayed_replicas,
						IFNULL(SUM(replica_instance.subordinate_lag_seconds > ?),
              0) AS count_lagging_replicas,
						IFNULL(MIN(replica_instance.gtid_mode), '')
              AS min_replica_gtid_mode,
						IFNULL(MAX(replica_instance.gtid_mode), '')
              AS max_replica_gtid_mode,
						IFNULL(MAX(
								case
									when replica_downtime.downtime_active is not null and ifnull(replica_downtime.end_timestamp, now()) > now() then ''
									else replica_instance.gtid_errant
								end
							), '') AS max_replica_gtid_errant,
						IFNULL(SUM(
								replica_downtime.downtime_active is not null
								and ifnull(replica_downtime.end_timestamp, now()) > now()),
              0) AS count_downtimed_replicas,
						COUNT(DISTINCT case
								when replica_instance.log_bin AND replica_instance.log_subordinate_updates
								then replica_instance.major_version
								else NULL
							end
						) AS count_distinct_logging_major_versions
		    FROM
		        database_instance main_instance
          LEFT JOIN
		        hostname_resolve ON (main_instance.hostname = hostname_resolve.hostname)
          LEFT JOIN
		        database_instance replica_instance ON (COALESCE(hostname_resolve.resolved_hostname,
              main_instance.hostname) = replica_instance.main_host
							AND main_instance.port = replica_instance.main_port)
          LEFT JOIN
		        database_instance_maintenance ON (main_instance.hostname = database_instance_maintenance.hostname
							AND main_instance.port = database_instance_maintenance.port
							AND database_instance_maintenance.maintenance_active = 1)
          LEFT JOIN
		        database_instance_downtime as main_downtime ON (main_instance.hostname = main_downtime.hostname
							AND main_instance.port = main_downtime.port
							AND main_downtime.downtime_active = 1)
					LEFT JOIN
		        database_instance_downtime as replica_downtime ON (replica_instance.hostname = replica_downtime.hostname
							AND replica_instance.port = replica_downtime.port
							AND replica_downtime.downtime_active = 1)
        	LEFT JOIN
		        cluster_alias ON (cluster_alias.cluster_name = main_instance.cluster_name)
		    WHERE
		    	database_instance_maintenance.database_instance_maintenance_id IS NULL
		    	AND ? IN ('', main_instance.cluster_name)
		    GROUP BY
			    main_instance.hostname,
			    main_instance.port
			%s
		    ORDER BY
			    is_main DESC ,
			    is_cluster_main DESC,
			    count_replicas DESC
	`, analysisQueryReductionClause)
	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		a := ReplicationAnalysis{
			Analysis:               NoProblem,
			ProcessingNodeHostname: process.ThisHostname,
			ProcessingNodeToken:    util.ProcessToken.Hash,
		}

		a.IsMain = m.GetBool("is_main")
		a.IsCoMain = m.GetBool("is_co_main")
		a.AnalyzedInstanceKey = InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		a.AnalyzedInstanceMainKey = InstanceKey{Hostname: m.GetString("main_host"), Port: m.GetInt("main_port")}
		a.AnalyzedInstanceDataCenter = m.GetString("data_center")
		a.AnalyzedInstancePhysicalEnvironment = m.GetString("physical_environment")
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.GTIDMode = m.GetString("gtid_mode")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.LastCheckPartialSuccess = m.GetBool("last_check_partial_success")
		a.CountReplicas = m.GetUint("count_replicas")
		a.CountValidReplicas = m.GetUint("count_valid_subordinates")
		a.CountValidReplicatingReplicas = m.GetUint("count_valid_replicating_subordinates")
		a.CountReplicasFailingToConnectToMain = m.GetUint("count_replicas_failing_to_connect_to_main")
		a.CountDowntimedReplicas = m.GetUint("count_downtimed_replicas")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToMain = m.GetBool("is_failing_to_connect_to_main")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsBinlogServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.SubordinateHosts = *NewInstanceKeyMap()
		a.SubordinateHosts.ReadCommaDelimitedList(m.GetString("subordinate_hosts"))

		countValidOracleGTIDSubordinates := m.GetUint("count_valid_oracle_gtid_subordinates")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDSubordinates == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidMariaDBGTIDSubordinates := m.GetUint("count_valid_mariadb_gtid_subordinates")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDSubordinates == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidBinlogServerSubordinates := m.GetUint("count_valid_binlog_server_subordinates")
		a.BinlogServerImmediateTopology = countValidBinlogServerSubordinates == a.CountValidReplicas && a.CountValidReplicas > 0
		a.PseudoGTIDImmediateTopology = m.GetBool("is_pseudo_gtid")

		a.MinReplicaGTIDMode = m.GetString("min_replica_gtid_mode")
		a.MaxReplicaGTIDMode = m.GetString("max_replica_gtid_mode")
		a.MaxReplicaGTIDErrant = m.GetString("max_replica_gtid_errant")

		a.CountStatementBasedLoggingReplicas = m.GetUint("count_statement_based_loggin_subordinates")
		a.CountMixedBasedLoggingReplicas = m.GetUint("count_mixed_based_loggin_subordinates")
		a.CountRowBasedLoggingReplicas = m.GetUint("count_row_based_loggin_subordinates")
		a.CountDistinctMajorVersionsLoggingReplicas = m.GetUint("count_distinct_logging_major_versions")

		a.CountDelayedReplicas = m.GetUint("count_delayed_replicas")
		a.CountLaggingReplicas = m.GetUint("count_lagging_replicas")

		if !a.LastCheckValid {
			analysisMessage := fmt.Sprintf("analysis: IsMain: %+v, LastCheckValid: %+v, LastCheckPartialSuccess: %+v, CountReplicas: %+v, CountValidReplicatingReplicas: %+v, CountLaggingReplicas: %+v, CountDelayedReplicas: %+v, ",
				a.IsMain, a.LastCheckValid, a.LastCheckPartialSuccess, a.CountReplicas, a.CountValidReplicatingReplicas, a.CountLaggingReplicas, a.CountDelayedReplicas,
			)
			if util.ClearToLog("analysis_dao", analysisMessage) {
				log.Debugf(analysisMessage)
			}
		}
		if a.IsMain && !a.LastCheckValid && a.CountReplicas == 0 {
			a.Analysis = DeadMainWithoutSubordinates
			a.Description = "Main cannot be reached by orchestrator and has no subordinate"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadMain
			a.Description = "Main cannot be reached by orchestrator and none of its replicas is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadMainAndSubordinates
			a.Description = "Main cannot be reached by orchestrator and none of its replicas is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadMainAndSomeSubordinates
			a.Description = "Main cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
			a.Analysis = UnreachableMainWithLaggingReplicas
			a.Description = "Main cannot be reached by orchestrator and all of its replicas are lagging"
			//
		} else if a.IsMain && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
			a.Analysis = UnreachableMain
			a.Description = "Main cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = MainSingleSubordinateNotReplicating
			a.Description = "Main is reachable but its single subordinate is not replicating"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == 0 {
			a.Analysis = MainSingleSubordinateDead
			a.Description = "Main is reachable but its single subordinate is dead"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllMainSubordinatesNotReplicating
			a.Description = "Main is reachable but none of its replicas is replicating"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllMainSubordinatesNotReplicatingOrDead
			a.Description = "Main is reachable but none of its replicas is replicating"
			//
		} else /* co-main */ if a.IsCoMain && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadCoMain
			a.Description = "Co-main cannot be reached by orchestrator and none of its replicas is replicating"
			//
		} else if a.IsCoMain && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadCoMainAndSomeSubordinates
			a.Description = "Co-main cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
			//
		} else if a.IsCoMain && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
			a.Analysis = UnreachableCoMain
			a.Description = "Co-main cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
			//
		} else if a.IsCoMain && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllCoMainSubordinatesNotReplicating
			a.Description = "Co-main is reachable but none of its replicas is replicating"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountReplicasFailingToConnectToMain == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadIntermediateMainWithSingleSubordinateFailingToConnect
			a.Description = "Intermediate main cannot be reached by orchestrator and its (single) subordinate is failing to connect"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadIntermediateMainWithSingleSubordinate
			a.Description = "Intermediate main cannot be reached by orchestrator and its (single) subordinate is not replicating"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadIntermediateMain
			a.Description = "Intermediate main cannot be reached by orchestrator and none of its replicas is replicating"
			//
		} else if !a.IsMain && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadIntermediateMainAndSomeSubordinates
			a.Description = "Intermediate main cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
			//
		} else if !a.IsMain && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 {
			a.Analysis = DeadIntermediateMainAndSubordinates
			a.Description = "Intermediate main cannot be reached by orchestrator and all of its replicas are unreachable"
			//
		} else if !a.IsMain && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
			a.Analysis = UnreachableIntermediateMain
			a.Description = "Intermediate main cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
			//
		} else if !a.IsMain && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicatingReplicas == 0 &&
			a.CountReplicasFailingToConnectToMain > 0 && a.CountReplicasFailingToConnectToMain == a.CountValidReplicas {
			// All replicas are either failing to connect to main (and at least one of these have to exist)
			// or completely dead.
			// Must have at least two replicas to reach such conclusion -- do note that the intermediate main is still
			// reachable to orchestrator, so we base our conclusion on replicas only at this point.
			a.Analysis = AllIntermediateMainSubordinatesFailingToConnectOrDead
			a.Description = "Intermediate main is reachable but all of its replicas are failing to connect"
			//
		} else if !a.IsMain && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllIntermediateMainSubordinatesNotReplicating
			a.Description = "Intermediate main is reachable but none of its replicas is replicating"
			//
		} else if a.IsBinlogServer && a.IsFailingToConnectToMain {
			a.Analysis = BinlogServerFailingToConnectToMain
			a.Description = "Binlog server is unable to connect to its main"
			//
		} else if a.ReplicationDepth == 1 && a.IsFailingToConnectToMain {
			a.Analysis = FirstTierSubordinateFailingToConnectToMain
			a.Description = "1st tier subordinate (directly replicating from topology main) is unable to connect to the main"
			//
		}
		//		 else if a.IsMain && a.CountReplicas == 0 {
		//			a.Analysis = MainWithoutSubordinates
		//			a.Description = "Main has no replicas"
		//		}

		appendAnalysis := func(analysis *ReplicationAnalysis) {
			if a.Analysis == NoProblem && len(a.StructureAnalysis) == 0 && !hints.IncludeNoProblem {
				return
			}
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					return
				}
			}
			if a.IsDowntimed {
				a.SkippableDueToDowntime = true
			}
			if a.CountReplicas == a.CountDowntimedReplicas {
				switch a.Analysis {
				case AllMainSubordinatesNotReplicating,
					AllMainSubordinatesNotReplicatingOrDead,
					MainSingleSubordinateDead,
					AllCoMainSubordinatesNotReplicating,
					DeadIntermediateMainWithSingleSubordinate,
					DeadIntermediateMainWithSingleSubordinateFailingToConnect,
					DeadIntermediateMainAndSubordinates,
					DeadIntermediateMainAndSomeSubordinates,
					AllIntermediateMainSubordinatesFailingToConnectOrDead,
					AllIntermediateMainSubordinatesNotReplicating:
					a.IsReplicasDowntimed = true
					a.SkippableDueToDowntime = true
				}
			}
			if a.SkippableDueToDowntime && !hints.IncludeDowntimed {
				return
			}
			result = append(result, a)
		}

		{
			// Moving on to structure analysis
			// We also do structural checks. See if there's potential danger in promotions
			if a.IsMain && a.CountStatementBasedLoggingReplicas > 0 && a.CountMixedBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndMixedLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountStatementBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndRowLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountMixedBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, MixedAndRowLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountDistinctMajorVersionsLoggingReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, MultipleMajorVersionsLoggingSubordinates)
			}

			if a.CountReplicas > 0 && (a.GTIDMode != a.MinReplicaGTIDMode || a.GTIDMode != a.MaxReplicaGTIDMode) {
				a.StructureAnalysis = append(a.StructureAnalysis, DifferentGTIDModesStructureWarning)
			}
			if a.MaxReplicaGTIDErrant != "" {
				a.StructureAnalysis = append(a.StructureAnalysis, ErrantGTIDStructureWarning)
			}
		}
		appendAnalysis(&a)

		if a.CountReplicas > 0 && hints.AuditAnalysis {
			// Interesting enough for analysis
			go auditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		return result, log.Errore(err)
	}
	// TODO: result, err = getConcensusReplicationAnalysis(result)
	return result, log.Errore(err)
}

func getConcensusReplicationAnalysis(analysisEntries []ReplicationAnalysis) ([]ReplicationAnalysis, error) {
	if !orcraft.IsRaftEnabled() {
		return analysisEntries, nil
	}
	if !config.Config.ExpectFailureAnalysisConcensus {
		return analysisEntries, nil
	}
	concensusAnalysisEntries := []ReplicationAnalysis{}
	peerAnalysisMap, err := ReadPeerAnalysisMap()
	if err != nil {
		return analysisEntries, err
	}
	quorumSize, err := orcraft.QuorumSize()
	if err != nil {
		return analysisEntries, err
	}

	for _, analysisEntry := range analysisEntries {
		instanceAnalysis := NewInstanceAnalysis(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		analysisKey := instanceAnalysis.String()

		peerAnalysisCount := peerAnalysisMap[analysisKey]
		if 1+peerAnalysisCount >= quorumSize {
			// this node and enough other nodes in agreement
			concensusAnalysisEntries = append(concensusAnalysisEntries, analysisEntry)
		}
	}
	return concensusAnalysisEntries, nil
}

// auditInstanceAnalysisInChangelog will write down an instance's analysis in the database_instance_analysis_changelog table.
// To not repeat recurring analysis code, the database_instance_last_analysis table is used, so that only changes to
// analysis codes are written.
func auditInstanceAnalysisInChangelog(instanceKey *InstanceKey, analysisCode AnalysisCode) error {
	if lastWrittenAnalysis, found := recentInstantAnalysis.Get(instanceKey.DisplayString()); found {
		if lastWrittenAnalysis == analysisCode {
			// Surely nothing new.
			// And let's expand the timeout
			recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
			return nil
		}
	}
	// Passed the cache; but does database agree that there's a change? Here's a persistent cache; this comes here
	// to verify no two orchestrator services are doing this without coordinating (namely, one dies, the other taking its place
	// and has no familiarity of the former's cache)
	analysisChangeWriteAttemptCounter.Inc(1)

	lastAnalysisChanged := false
	{
		sqlResult, err := db.ExecOrchestrator(`
			update database_instance_last_analysis set
				analysis = ?,
				analysis_timestamp = now()
			where
				hostname = ?
				and port = ?
				and analysis != ?
			`,
			string(analysisCode), instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			return log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return log.Errore(err)
		}
		lastAnalysisChanged = (rows > 0)
	}
	if !lastAnalysisChanged {
		_, err := db.ExecOrchestrator(`
			insert ignore into database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
			instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			return log.Errore(err)
		}
	}
	recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
	if !lastAnalysisChanged {
		return nil
	}

	_, err := db.ExecOrchestrator(`
			insert into database_instance_analysis_changelog (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	return log.Errore(err)
}

// ExpireInstanceAnalysisChangelog removes old-enough analysis entries from the changelog
func ExpireInstanceAnalysisChangelog() error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance_analysis_changelog
			where
				analysis_timestamp < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	return log.Errore(err)
}

// ReadReplicationAnalysisChangelog
func ReadReplicationAnalysisChangelog() (res [](*ReplicationAnalysisChangelog), err error) {
	query := `
		select
      hostname,
      port,
			analysis_timestamp,
			analysis
		from
			database_instance_analysis_changelog
		order by
			hostname, port, changelog_id
		`
	analysisChangelog := &ReplicationAnalysisChangelog{}
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		key := InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}

		if !analysisChangelog.AnalyzedInstanceKey.Equals(&key) {
			analysisChangelog = &ReplicationAnalysisChangelog{AnalyzedInstanceKey: key, Changelog: []string{}}
			res = append(res, analysisChangelog)
		}
		analysisEntry := fmt.Sprintf("%s;%s,", m.GetString("analysis_timestamp"), m.GetString("analysis"))
		analysisChangelog.Changelog = append(analysisChangelog.Changelog, analysisEntry)

		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// ReadPeerAnalysisMap reads raft-peer failure analysis, and returns a PeerAnalysisMap,
// indicating how many peers see which analysis
func ReadPeerAnalysisMap() (peerAnalysisMap PeerAnalysisMap, err error) {
	peerAnalysisMap = make(PeerAnalysisMap)
	query := `
		select
      hostname,
      port,
			analysis
		from
			database_instance_peer_analysis
		order by
			peer, hostname, port
		`
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		instanceKey := InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		analysis := m.GetString("analysis")
		instanceAnalysis := NewInstanceAnalysis(&instanceKey, AnalysisCode(analysis))
		mapKey := instanceAnalysis.String()
		peerAnalysisMap[mapKey] = peerAnalysisMap[mapKey] + 1

		return nil
	})
	return peerAnalysisMap, log.Errore(err)
}

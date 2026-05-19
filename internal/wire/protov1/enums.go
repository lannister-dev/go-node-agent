package protov1

import (
	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

func desiredFromProto(p agentv1.DesiredState) domain.DesiredState {
	switch p {
	case agentv1.DesiredState_DESIRED_STATE_ACTIVE:
		return domain.DesiredActive
	case agentv1.DesiredState_DESIRED_STATE_INACTIVE:
		return domain.DesiredInactive
	default:
		return ""
	}
}

func appliedToProto(a domain.AppliedState) agentv1.AppliedState {
	switch a {
	case domain.AppliedPending:
		return agentv1.AppliedState_APPLIED_STATE_PENDING
	case domain.AppliedOk:
		return agentv1.AppliedState_APPLIED_STATE_APPLIED
	case domain.AppliedError:
		return agentv1.AppliedState_APPLIED_STATE_ERROR
	default:
		return agentv1.AppliedState_APPLIED_STATE_UNSPECIFIED
	}
}

func reportStatusToProto(r domain.ReportStatus) agentv1.ReportStatus {
	switch r {
	case domain.ReportApplied:
		return agentv1.ReportStatus_REPORT_STATUS_APPLIED
	case domain.ReportPending:
		return agentv1.ReportStatus_REPORT_STATUS_PENDING
	case domain.ReportError:
		return agentv1.ReportStatus_REPORT_STATUS_ERROR
	case domain.ReportSkippedStale:
		return agentv1.ReportStatus_REPORT_STATUS_SKIPPED_STALE
	case domain.ReportSkippedIdempotent:
		return agentv1.ReportStatus_REPORT_STATUS_SKIPPED_IDEMPOTENT
	default:
		return agentv1.ReportStatus_REPORT_STATUS_UNSPECIFIED
	}
}

func transportFromProto(p agentv1.TransportKind) domain.TransportKind {
	switch p {
	case agentv1.TransportKind_TRANSPORT_KIND_WS:
		return domain.TransportWS
	case agentv1.TransportKind_TRANSPORT_KIND_XHTTP:
		return domain.TransportXHTTP
	case agentv1.TransportKind_TRANSPORT_KIND_TCP:
		return domain.TransportTCP
	case agentv1.TransportKind_TRANSPORT_KIND_REALITY:
		return domain.TransportReality
	default:
		return ""
	}
}

func protocolFromProto(p agentv1.Protocol) domain.Protocol {
	if p == agentv1.Protocol_PROTOCOL_VLESS {
		return domain.ProtocolVLESS
	}
	return ""
}

const (
	SnapshotReasonStartup        = "startup"
	SnapshotReasonXrayRestart    = "xray_restart"
	SnapshotReasonRedeliveryGap  = "redelivery_gap"
	SnapshotReasonOperatorForced = "operator_forced"
)

func snapshotReasonToProto(r string) agentv1.SnapshotRequestReason {
	switch r {
	case SnapshotReasonStartup:
		return agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_STARTUP
	case SnapshotReasonXrayRestart:
		return agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_XRAY_RESTART
	case SnapshotReasonRedeliveryGap:
		return agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_REDELIVERY_GAP
	case SnapshotReasonOperatorForced:
		return agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_OPERATOR_FORCED
	default:
		return agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_UNSPECIFIED
	}
}

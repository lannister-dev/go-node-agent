package jsonv1

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestMarshalApplyResultEvent_Success(t *testing.T) {
	r := domain.PlacementReport{
		PlacementID:  "p-001",
		NodeID:       "lv-01",
		OpVersion:    7,
		AppliedState: domain.AppliedOk,
		Status:       domain.ReportApplied,
	}
	at := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	raw, err := MarshalApplyResultEvent(r, "evt-result-1", at)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["applied_state"] != "applied" || got["report_status"] != "applied" {
		t.Errorf("states: %+v", got)
	}
	if got["op_version"].(float64) != 7 {
		t.Errorf("op_version: %v", got["op_version"])
	}
	if _, ok := got["error"]; ok {
		t.Errorf("error should be omitted when empty")
	}
	if got["retryable"].(bool) != false {
		t.Errorf("retryable: %v", got["retryable"])
	}
	if got["emitted_at"].(string) != "2026-05-19T12:00:00Z" {
		t.Errorf("emitted_at: %v", got["emitted_at"])
	}
}

func TestMarshalApplyResultEvent_Error(t *testing.T) {
	r := domain.PlacementReport{
		PlacementID:  "p-001",
		NodeID:       "lv-01",
		OpVersion:    7,
		AppliedState: domain.AppliedError,
		Status:       domain.ReportError,
		Retryable:    true,
		Err:          "xray down",
	}
	raw, err := MarshalApplyResultEvent(r, "evt-result-2", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["error"].(string) != "xray down" || got["retryable"].(bool) != true {
		t.Errorf("error/retryable: %+v", got)
	}
}

func TestMarshalApplyResultEvent_SkippedStale(t *testing.T) {
	r := domain.PlacementReport{
		PlacementID:  "p-001",
		NodeID:       "lv-01",
		OpVersion:    3,
		AppliedState: domain.AppliedOk,
		Status:       domain.ReportSkippedStale,
	}
	raw, _ := MarshalApplyResultEvent(r, "e", time.Now())
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["report_status"] != "skipped_stale" {
		t.Errorf("report_status: %v", got["report_status"])
	}
}

func TestMarshalApplyResultEvent_RejectsInvalid(t *testing.T) {
	cases := map[string]domain.PlacementReport{
		"missing id":        {NodeID: "n", OpVersion: 1, AppliedState: domain.AppliedOk},
		"missing node":      {PlacementID: "p", OpVersion: 1, AppliedState: domain.AppliedOk},
		"zero op_version":   {PlacementID: "p", NodeID: "n", AppliedState: domain.AppliedOk},
		"bad applied_state": {PlacementID: "p", NodeID: "n", OpVersion: 1, AppliedState: "bad"},
		"bad report_status": {PlacementID: "p", NodeID: "n", OpVersion: 1, AppliedState: domain.AppliedOk, Status: "bad"},
	}
	for name, r := range cases {
		if _, err := MarshalApplyResultEvent(r, "e", time.Now()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// Package lifecycle implements conservative worker lifecycle admission decisions.
// It is not registered with ARC's manager. Production wiring requires durable
// broker authority and worker-side epoch checks in addition to these predicates.
package lifecycle

import "time"

type Action string

const (
	Hold                  Action = "Hold"
	CreateInitial         Action = "CreateInitial"
	DeleteCommittedWorker Action = "DeleteCommittedWorker"
	CreateResume          Action = "CreateResume"
	CreateFinalizer       Action = "CreateFinalizer"
)

type Snapshot struct {
	Phase                       string
	ExpectedEpoch               int64
	OwnerEpoch                  int64
	OwnerUntil                  time.Time
	JobDeadline                 time.Time
	ExpectedWorkspaceGeneration int64
	WorkspaceGeneration         int64
	ActiveWorker                bool
	PreviousWorkerDeleted       bool
	WorkspaceReleased           bool
	CheckpointCommitted         bool
	CallbackRecorded            bool
	CancelRequested             bool
}

// Decide must be followed by an atomic claim in the broker's authoritative store.
// It cannot fence Kubernetes writes or GitHub calls by itself.
func Decide(s Snapshot, now time.Time) Action {
	if s.ExpectedEpoch < 1 || s.ExpectedEpoch != s.OwnerEpoch || !now.Before(s.OwnerUntil) {
		return Hold
	}
	if s.ExpectedWorkspaceGeneration < 1 || s.ExpectedWorkspaceGeneration != s.WorkspaceGeneration {
		return Hold
	}
	switch s.Phase {
	case "Succeeded", "Failed", "Cancelled", "Lost", "Expired":
		return Hold
	}
	if s.CancelRequested || !now.Before(s.JobDeadline) {
		if s.ActiveWorker || !s.CheckpointCommitted || !s.PreviousWorkerDeleted || !s.WorkspaceReleased {
			return Hold
		}
		return CreateFinalizer
	}
	if s.Phase == "Acquired" && !s.ActiveWorker {
		return CreateInitial
	}
	if s.Phase == "Suspended" && s.CheckpointCommitted && s.ActiveWorker {
		return DeleteCommittedWorker
	}
	if s.Phase == "ResumeRequested" && s.CheckpointCommitted && s.CallbackRecorded && !s.ActiveWorker && s.PreviousWorkerDeleted && s.WorkspaceReleased {
		return CreateResume
	}
	return Hold
}

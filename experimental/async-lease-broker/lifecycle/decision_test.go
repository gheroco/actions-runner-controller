package lifecycle

import (
	"testing"
	"time"
)

func TestResumeRequiresAllSafetyConditions(t *testing.T) {
	now := time.Unix(1000, 0)
	good := Snapshot{Phase: "ResumeRequested", ExpectedEpoch: 2, OwnerEpoch: 2, OwnerUntil: now.Add(time.Minute), JobDeadline: now.Add(time.Hour), ExpectedWorkspaceGeneration: 1, WorkspaceGeneration: 1, PreviousWorkerDeleted: true, WorkspaceReleased: true, CheckpointCommitted: true, CallbackRecorded: true}
	if got := Decide(good, now); got != CreateResume {
		t.Fatal(got)
	}
	cases := []func(*Snapshot){
		func(s *Snapshot) { s.ExpectedEpoch = 1 },
		func(s *Snapshot) { s.OwnerUntil = now },
		func(s *Snapshot) { s.ExpectedWorkspaceGeneration = 2 },
		func(s *Snapshot) { s.ActiveWorker = true },
		func(s *Snapshot) { s.PreviousWorkerDeleted = false },
		func(s *Snapshot) { s.WorkspaceReleased = false },
		func(s *Snapshot) { s.CheckpointCommitted = false },
		func(s *Snapshot) { s.CallbackRecorded = false },
		func(s *Snapshot) { s.Phase = "Lost" },
	}
	for i, change := range cases {
		s := good
		change(&s)
		if got := Decide(s, now); got != Hold {
			t.Fatalf("case %d: %s", i, got)
		}
	}
	good.CancelRequested = true
	if got := Decide(good, now); got != CreateFinalizer {
		t.Fatal(got)
	}
	good.CancelRequested = false
	good.JobDeadline = now
	if got := Decide(good, now); got != CreateFinalizer {
		t.Fatal(got)
	}
}

func TestDeletionOnlyAfterCommit(t *testing.T) {
	now := time.Unix(1000, 0)
	s := Snapshot{Phase: "SuspendPrepared", ExpectedEpoch: 1, OwnerEpoch: 1, OwnerUntil: now.Add(time.Minute), JobDeadline: now.Add(time.Hour), ExpectedWorkspaceGeneration: 1, WorkspaceGeneration: 1, ActiveWorker: true}
	if got := Decide(s, now); got != Hold {
		t.Fatal(got)
	}
	s.Phase = "Suspended"
	s.CheckpointCommitted = true
	if got := Decide(s, now); got != DeleteCommittedWorker {
		t.Fatal(got)
	}
}

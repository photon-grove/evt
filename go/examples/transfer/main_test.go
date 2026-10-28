package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

func TestRunPrintsEachOutcome(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	want := `transfer-1: accepted {"fromBalance":70,"toBalance":50} replayed=false
transfer-1 retried: accepted {"fromBalance":70,"toBalance":50} replayed=true
transfer-2 overdraws: rejected (insufficient-funds) replayed=false
transfer-2 retried: rejected (insufficient-funds) replayed=true
transfer-1 with another amount: idempotency key reuse
acct-1: balance=70
acct-2: balance=50
`
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

// seeded returns a store with acct-1 (100) and acct-2 (20) open.
func seeded(t *testing.T) (context.Context, *snapshots.Store) {
	t.Helper()

	ctx := context.Background()
	store := newStore()

	for id, initial := range map[evt.EntityID]int64{"acct-1": 100, "acct-2": 20} {
		if err := open(ctx, store, id, initial); err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
	}

	return ctx, store
}

func assertBalances(ctx context.Context, t *testing.T, store *snapshots.Store, want map[evt.EntityID]int64) {
	t.Helper()

	for id, expected := range want {
		got, err := balance(ctx, store, id)
		if err != nil {
			t.Fatal(err)
		}

		if got != expected {
			t.Fatalf("%s balance = %d, want %d", id, got, expected)
		}
	}
}

func TestATransferWritesBothStreamsInOneCommit(t *testing.T) {
	ctx, store := seeded(t)

	outcome, err := Transfer(ctx, store, "transfer-1", TransferRequest{From: "acct-1", To: "acct-2", Amount: 30})
	if err != nil {
		t.Fatal(err)
	}

	if !outcome.Accepted() || outcome.Replayed {
		t.Fatalf("outcome = %+v, want a fresh acceptance", outcome)
	}

	if len(outcome.Events) != 2 {
		t.Fatalf("committed %d events, want 2", len(outcome.Events))
	}

	commitID := outcome.Events[0].Metadata.CommitID
	if commitID == "" || outcome.Events[1].Metadata.CommitID != commitID {
		t.Fatalf("the debit and the credit carry different commit IDs: %q, %q",
			commitID, outcome.Events[1].Metadata.CommitID)
	}

	assertBalances(ctx, t, store, map[evt.EntityID]int64{"acct-1": 70, "acct-2": 50})
}

func TestARetryReplaysTheStoredOutcomeAndWritesNothing(t *testing.T) {
	ctx, store := seeded(t)
	request := TransferRequest{From: "acct-1", To: "acct-2", Amount: 30}

	first, err := Transfer(ctx, store, "transfer-1", request)
	if err != nil {
		t.Fatal(err)
	}

	again, err := Transfer(ctx, store, "transfer-1", request)
	if err != nil {
		t.Fatal(err)
	}

	if !again.Replayed || len(again.Events) != 0 {
		t.Fatalf("retry = %+v, want a replay with no new events", again)
	}

	if !bytes.Equal(first.Response, again.Response) {
		t.Fatalf("replayed response %s, want %s", again.Response, first.Response)
	}

	assertBalances(ctx, t, store, map[evt.EntityID]int64{"acct-1": 70, "acct-2": 50})
}

func TestTheSameCommandIDWithADifferentRequestIsRefused(t *testing.T) {
	ctx, store := seeded(t)

	if _, err := Transfer(ctx, store, "transfer-1", TransferRequest{From: "acct-1", To: "acct-2", Amount: 30}); err != nil {
		t.Fatal(err)
	}

	_, err := Transfer(ctx, store, "transfer-1", TransferRequest{From: "acct-1", To: "acct-2", Amount: 31})

	var reuse *evt.IdempotencyKeyReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("expected idempotency-key reuse, got %T: %v", err, err)
	}

	if !reuse.Prior.Accepted() {
		t.Fatalf("the error carries the stored outcome: %+v", reuse.Prior)
	}

	assertBalances(ctx, t, store, map[evt.EntityID]int64{"acct-1": 70, "acct-2": 50})
}

func TestAnOverdraftIsARecordedRejection(t *testing.T) {
	ctx, store := seeded(t)

	outcome, err := Transfer(ctx, store, "transfer-1", TransferRequest{From: "acct-2", To: "acct-1", Amount: 500})
	if err != nil {
		t.Fatal(err)
	}

	if !outcome.Rejected() || outcome.Rejection.Code != InsufficientFundsCode {
		t.Fatalf("outcome = %+v, want an insufficient-funds rejection", outcome)
	}

	stored, err := store.Outcome(ctx, "transfer-1")
	if err != nil {
		t.Fatal(err)
	}

	if stored == nil || !stored.Rejected() {
		t.Fatalf("stored outcome = %+v, want the rejection", stored)
	}

	assertBalances(ctx, t, store, map[evt.EntityID]int64{"acct-1": 100, "acct-2": 20})
}

func TestTransfersInvolvingAnUnopenedAccountOrABadAmountAreRejected(t *testing.T) {
	ctx, store := seeded(t)

	for _, tc := range []struct {
		commandID evt.CommandID
		request   TransferRequest
		code      evt.RejectionCode
	}{
		{"t-missing", TransferRequest{From: "acct-1", To: "acct-9", Amount: 1}, evt.RejectionNotFound},
		{"t-zero", TransferRequest{From: "acct-1", To: "acct-2", Amount: 0}, InvalidTransferCode},
		{"t-self", TransferRequest{From: "acct-1", To: "acct-1", Amount: 1}, InvalidTransferCode},
	} {
		outcome, err := Transfer(ctx, store, tc.commandID, tc.request)
		if err != nil {
			t.Fatalf("%s: %v", tc.commandID, err)
		}

		if !outcome.Rejected() || outcome.Rejection.Code != tc.code {
			t.Fatalf("%s: outcome = %+v, want %s", tc.commandID, outcome, tc.code)
		}
	}

	assertBalances(ctx, t, store, map[evt.EntityID]int64{"acct-1": 100, "acct-2": 20})
}

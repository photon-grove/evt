package main

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
)

func TestRunPrintsTheSummariesAndAMatchingRebuild(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	want := "list-1: open=1 done=1\n" +
		"list-2: open=1 done=0\n" +
		"rebuilt 2 rows from 4 commits; equal to the live rows: true\n"
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestTheSummaryRowCommitsWithEachCommand(t *testing.T) {
	ctx := context.Background()

	repo, err := newRepository()
	if err != nil {
		t.Fatal(err)
	}

	store := newStore(repo)

	if err := store.Execute(ctx, NewTodoList("list-1"), "list-1", &AddItem{ItemID: "a", Title: "A"}, evt.Metadata{}); err != nil {
		t.Fatalf("add: %v", err)
	}

	assertSummary(t, repo, "list-1", Summary{Open: 1})

	if err := store.Execute(ctx, NewTodoList("list-1"), "list-1", &CompleteItem{ItemID: "a"}, evt.Metadata{}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	assertSummary(t, repo, "list-1", Summary{Done: 1})

	row, ok := repo.View(summaryPrefix+"list-1", evt.DefaultViewSK)
	if !ok || row.EntityID != "list-1" {
		t.Fatalf("the row is owned by its list's stream: %+v", row)
	}
}

func TestARejectedCommandWritesNoEventAndNoRow(t *testing.T) {
	ctx := context.Background()

	repo, err := newRepository()
	if err != nil {
		t.Fatal(err)
	}

	store := newStore(repo)

	err = store.Execute(ctx, NewTodoList("list-1"), "list-1", &CompleteItem{ItemID: "missing"}, evt.Metadata{})
	if !evt.IsNotFoundErr(err) {
		t.Fatalf("expected a not-found error, got %T: %v", err, err)
	}

	if views := repo.Views(); len(views) != 0 {
		t.Fatalf("a rejected command wrote rows: %+v", views)
	}

	events, err := repo.GetEvents(ctx, "list-1")
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 0 {
		t.Fatalf("a rejected command wrote events: %+v", events)
	}
}

func TestCompletingAnItemTwiceConflicts(t *testing.T) {
	ctx := context.Background()

	repo, err := newRepository()
	if err != nil {
		t.Fatal(err)
	}

	store := newStore(repo)

	for _, command := range []evt.Command{&AddItem{ItemID: "a"}, &CompleteItem{ItemID: "a"}} {
		if err = store.Execute(ctx, NewTodoList("list-1"), "list-1", command, evt.Metadata{}); err != nil {
			t.Fatalf("%s: %v", command.Type(), err)
		}
	}

	err = store.Execute(ctx, NewTodoList("list-1"), "list-1", &CompleteItem{ItemID: "a"}, evt.Metadata{})
	if !evt.IsConflictErr(err) {
		t.Fatalf("expected a conflict, got %T: %v", err, err)
	}

	assertSummary(t, repo, "list-1", Summary{Done: 1})
}

func TestTheRebuildReproducesTheLiveRowsFromTheLogAlone(t *testing.T) {
	ctx := context.Background()

	repo, err := newRepository()
	if err != nil {
		t.Fatal(err)
	}

	if err = seed(ctx, newStore(repo)); err != nil {
		t.Fatal(err)
	}

	rebuilt, commits, err := RebuildSummaries(ctx, repo)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	if commits != 4 {
		t.Fatalf("commits = %d, want 4", commits)
	}

	live := repo.Views()
	if len(live) != 2 || !reflect.DeepEqual(rebuilt, live) {
		t.Fatalf("rebuilt rows differ from the live rows:\nrebuilt %+v\nlive    %+v", rebuilt, live)
	}
}

func assertSummary(t *testing.T, repo *mem.Repository, list evt.EntityID, want Summary) {
	t.Helper()

	got, ok, err := ReadSummary(repo, list)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("no summary row for %s", list)
	}

	if got != want {
		t.Fatalf("summary of %s = %+v, want %+v", list, got, want)
	}
}

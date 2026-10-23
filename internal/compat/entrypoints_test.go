package compat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// stubGroup is a backend-neutral projector group with one operation.
type stubGroup struct{}

func (stubGroup) Len() int                             { return 1 }
func (stubGroup) StorageType() evt.StorageType         { return "stub" }
func (stubGroup) TransactionType() evt.TransactionType { return "stub" }
func (stubGroup) HandleError(err error, _ int) error   { return err }
func (g stubGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

// TestEveryWriteEntryPointRefusesProjectorOnlyInput pins ADR-0012 across the public write paths of
// every Go backend: projector operations with no backing event are refused with the typed error,
// whichever entry point carries them, and nothing is written.
func TestEveryWriteEntryPointRefusesProjectorOnlyInput(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeDynamo()

	backends := map[string]evt.Repository{
		"mem":      mem.NewRepository(),
		"dynamo":   dynamo.NewRepository(fake, "events").WithLazyInitialization(true),
		"postgres": postgres.NewRepository(nil), // no pool: the refusal must precede any use of it
	}

	projectorOnly := evt.SerializedResult{Transaction: evt.Transaction{stubGroup{}}}

	entryPoints := map[string]func(evt.Repository) []error{
		"Commit": func(repo evt.Repository) []error {
			return []error{repo.Commit(ctx, projectorOnly)}
		},
		"CommitWithSnapshot": func(repo evt.Repository) []error {
			return []error{repo.CommitWithSnapshot(ctx, projectorOnly, "t", "entry-point", []byte(`{}`), 1)}
		},
		"CommitStream": func(repo evt.Repository) []error {
			channel := make(chan result.Result[evt.SerializedResult], 1)
			channel <- result.Ok(projectorOnly)
			close(channel)

			return repo.CommitStream(ctx, channel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts); this test pins its fail-closed behavior
		},
	}

	for backend, repo := range backends {
		for entryPoint, run := range entryPoints {
			t.Run(backend+"/"+entryPoint, func(t *testing.T) {
				errs := run(repo)

				// The DynamoDB batch path writes events with no head update, so it is refused outright
				// (ADR 0007), whatever it is handed.
				if backend == "dynamo" && entryPoint == "CommitStream" {
					require.Len(t, errs, 1)
					require.ErrorIs(t, errs[0], dynamo.ErrCommitStreamRefused)

					return
				}

				require.Len(t, errs, 1)
				var refused *evt.ProjectorOnlyCommitError
				require.ErrorAs(t, errs[0], &refused)
				require.Equal(t, 1, refused.Operations)
			})
		}
	}

	require.Empty(t, fake.Transactions(), "nothing reached the table")
}

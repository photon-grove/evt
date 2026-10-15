package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jackc/pgx/v5"

	"github.com/photon-grove/evt"
)

// reportReservedStreams reports every pre-existing stream whose entity ID falls in a reserved
// namespace (storage spec, Reserved Namespaces). Entity IDs were never restricted before, so a
// stream such as SYSTEM#account would become silently invisible to enumeration, entity heads, and
// rebuild scans once the v0.9 features are enabled; the upgrade procedure migrates (renames) these
// streams first, so real stored data is never silently skipped.
//
// Engine-written rows are exempt: the feature marker, the rebuild fence, and command outcomes live
// in the reserved namespaces by design and are reported by their own diagnostics, not as streams
// needing migration.
func reportReservedStreams(ids []evt.EntityID) {
	if len(ids) == 0 {
		fmt.Println("reserved-namespace check: no pre-existing streams in reserved namespaces")

		return
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	fmt.Printf("reserved-namespace check: %d stream(s) must be renamed (migrated) before enabling the v0.9 features:\n", len(ids))
	for _, id := range ids {
		fmt.Printf("  %s\n", id)
	}
}

// reservedStreamIDs scans the DynamoDB event log and returns the distinct entity IDs in reserved
// namespaces that are not engine-written rows.
func reservedStreamIDs(ctx context.Context, client dynamodb.ScanAPIClient, eventsTable string) ([]evt.EntityID, error) {
	// version, owner, leaseExpiresAt, and value are DynamoDB reserved words, so they are reached
	// through ExpressionAttributeNames aliases.
	input := dynamodb.ScanInput{
		TableName:            &eventsTable,
		ProjectionExpression: aws.String("pk, sk, #ver, #own, #lease, #val, #status"),
		ExpressionAttributeNames: map[string]string{
			"#ver":    "version",
			"#own":    "owner",
			"#lease":  "leaseExpiresAt",
			"#val":    "value",
			"#status": "status",
		},
	}

	reserved := make(map[evt.EntityID]struct{})

	p := dynamodb.NewScanPaginator(client, &input)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", eventsTable, err)
		}
		if page == nil {
			continue
		}

		for _, item := range page.Items {
			pkAttr, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}

			id := evt.EntityID(pkAttr.Value)
			if !evt.IsReservedEntityID(id) {
				continue
			}

			if isEngineRow(id, item) {
				continue
			}

			reserved[id] = struct{}{}
		}
	}

	ids := make([]evt.EntityID, 0, len(reserved))
	for id := range reserved {
		ids = append(ids, id)
	}

	return ids, nil
}

// isEngineRow reports whether a reserved-namespace row was written by the engine: the feature
// marker, the rebuild lock and generation, and command outcomes have stable shapes a user stream
// cannot accidentally mimic at the same key.
func isEngineRow(id evt.EntityID, item map[string]types.AttributeValue) bool {
	sk, ok := numAttrValue(item, "sk")
	if !ok {
		return false
	}

	switch {
	case string(id) == evt.SystemNamespacePrefix+"FEATURES":
		return sk == 0 && hasAttr(item, "version")
	case string(id) == evt.SystemNamespacePrefix+"REBUILD":
		return sk == 0 && hasAttr(item, "owner") && hasAttr(item, "leaseExpiresAt")
	case string(id) == evt.SystemNamespacePrefix+"REBUILD#GENERATION":
		return sk == 0 && hasAttr(item, "value")
	case strings.HasPrefix(string(id), evt.CommandOutcomeNamespacePrefix):
		return sk == 1 && hasAttr(item, "status")
	}

	return false
}

// hasAttr reports whether the item carries the named attribute of any type.
func hasAttr(item map[string]types.AttributeValue, name string) bool {
	_, ok := item[name]

	return ok
}

// numAttrValue reads an integer DynamoDB Number attribute, returning false when absent or
// non-numeric.
func numAttrValue(item map[string]types.AttributeValue, name string) (int64, bool) {
	attr, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}

	n, err := strconv.ParseInt(attr.Value, 10, 64)
	if err != nil {
		return 0, false
	}

	return n, true
}

type postgresQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// postgresReservedStreamIDs runs the storage spec's two queries against the configured events and
// snapshots tables and returns every entity ID they report. Engine-written rows are exempt
// structurally: the engine-row relations derive from the events table name through the reserved
// infix, so these queries never see them.
func postgresReservedStreamIDs(ctx context.Context, pool postgresQuerier, eventsTable, snapshotsTable string) ([]evt.EntityID, error) {
	if !validPgIdentifier(eventsTable) || !validPgIdentifier(snapshotsTable) {
		return nil, fmt.Errorf("invalid table name %q or %q", eventsTable, snapshotsTable)
	}

	seen := make(map[evt.EntityID]struct{})

	queries := []string{
		fmt.Sprintf(`SELECT DISTINCT entity_id FROM %s WHERE entity_id LIKE 'SYSTEM#%%' OR entity_id LIKE 'command-outcome#%%'`, eventsTable),
		fmt.Sprintf(`SELECT entity_id FROM %s WHERE entity_id LIKE 'SYSTEM#%%' OR entity_id LIKE 'command-outcome#%%'`, snapshotsTable),
	}

	for _, query := range queries {
		rows, err := pool.Query(ctx, query)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var id evt.EntityID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}

			seen[id] = struct{}{}
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}

		rows.Close()
	}

	ids := make([]evt.EntityID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids, nil
}

// validPgIdentifier reports whether a table name is a plain PostgreSQL identifier safe to
// interpolate unquoted. The backend repositories enforce the same rule.
func validPgIdentifier(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}

	for i, r := range name {
		alnum := r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !alnum || (i == 0 && r >= '0' && r <= '9') {
			return false
		}
	}

	return true
}

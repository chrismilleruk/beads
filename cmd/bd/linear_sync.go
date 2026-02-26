package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/linear"
	"github.com/steveyegge/beads/internal/types"
)

// doPullFromLinear imports issues from Linear using the GraphQL API.
// Supports incremental sync by checking linear.last_sync config and only fetching
// issues updated since that timestamp.
// Skips Linear issues with state type "canceled" (treated as deleted).
// excludeTypes filters out issues whose beads IssueType matches (from linear.exclude_types or --exclude-type).
func doPullFromLinear(ctx context.Context, dryRun bool, state string, skipLinearIDs map[string]bool, excludeTypes []string) (*linear.PullStats, error) {
	stats := &linear.PullStats{}

	client, err := getLinearClient(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to create Linear client: %w", err)
	}

	var linearIssues []linear.Issue
	lastSyncStr, _ := store.GetConfig(ctx, "linear.last_sync")

	if lastSyncStr != "" {
		lastSync, err := time.Parse(time.RFC3339, lastSyncStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: invalid linear.last_sync timestamp, doing full sync\n")
			linearIssues, err = client.FetchIssues(ctx, state)
			if err != nil {
				return stats, fmt.Errorf("failed to fetch issues from Linear: %w", err)
			}
		} else {
			stats.Incremental = true
			stats.SyncedSince = lastSyncStr
			linearIssues, err = client.FetchIssuesSince(ctx, state, lastSync)
			if err != nil {
				return stats, fmt.Errorf("failed to fetch issues from Linear (incremental): %w", err)
			}
			if !dryRun {
				fmt.Printf("  Incremental sync since %s\n", lastSync.Format("2006-01-02 15:04:05"))
			}
		}
	} else {
		linearIssues, err = client.FetchIssues(ctx, state)
		if err != nil {
			return stats, fmt.Errorf("failed to fetch issues from Linear: %w", err)
		}
		if !dryRun {
			fmt.Println("  Full sync (no previous sync timestamp)")
		}
	}

	mappingConfig := loadLinearMappingConfig(ctx)

	idMode := getLinearIDMode(ctx)
	hashLength := getLinearHashLength(ctx)

	var beadsIssues []*types.Issue
	var allDeps []linear.DependencyInfo
	linearIDToBeadsID := make(map[string]string)
	var canceledLinearIDs []string

	for i := range linearIssues {
		li := &linearIssues[i]
		// Skip canceled issues (treated as deleted in Linear)
		if li.State != nil {
			t := strings.ToLower(strings.TrimSpace(li.State.Type))
			if t == "canceled" || t == "cancelled" {
				stats.Skipped++
				canceledLinearIDs = append(canceledLinearIDs, li.Identifier)
				continue
			}
		}
		conversion := linear.IssueToBeads(li, mappingConfig)
		beadsIssues = append(beadsIssues, conversion.Issue.(*types.Issue))
		allDeps = append(allDeps, conversion.Dependencies...)
	}

	if len(beadsIssues) == 0 {
		fmt.Println("  No issues to import")
		return stats, nil
	}

	if len(skipLinearIDs) > 0 {
		var filteredIssues []*types.Issue
		skipped := 0
		for _, issue := range beadsIssues {
			if issue.ExternalRef == nil {
				filteredIssues = append(filteredIssues, issue)
				continue
			}
			linearID := linear.ExtractLinearIdentifier(*issue.ExternalRef)
			if linearID != "" && skipLinearIDs[linearID] {
				skipped++
				continue
			}
			filteredIssues = append(filteredIssues, issue)
		}
		if skipped > 0 {
			stats.Skipped += skipped
		}
		beadsIssues = filteredIssues

		if len(allDeps) > 0 {
			var filteredDeps []linear.DependencyInfo
			for _, dep := range allDeps {
				if skipLinearIDs[dep.FromLinearID] || skipLinearIDs[dep.ToLinearID] {
					continue
				}
				filteredDeps = append(filteredDeps, dep)
			}
			allDeps = filteredDeps
		}
	}

	// Filter by exclude types (linear.exclude_types or --exclude-type)
	if len(excludeTypes) > 0 {
		excludeSet := make(map[string]bool, len(excludeTypes))
		for _, t := range excludeTypes {
			excludeSet[strings.ToLower(t)] = true
		}
		var filteredIssues []*types.Issue
		excludedLinearIDs := make(map[string]bool)
		for _, issue := range beadsIssues {
			issueType := strings.ToLower(string(issue.IssueType))
			if excludeSet[issueType] {
				stats.Skipped++
				if issue.ExternalRef != nil {
					if id := linear.ExtractLinearIdentifier(*issue.ExternalRef); id != "" {
						excludedLinearIDs[id] = true
					}
				}
				continue
			}
			filteredIssues = append(filteredIssues, issue)
		}
		beadsIssues = filteredIssues
		if len(allDeps) > 0 && len(excludedLinearIDs) > 0 {
			var filteredDeps []linear.DependencyInfo
			for _, dep := range allDeps {
				if excludedLinearIDs[dep.FromLinearID] || excludedLinearIDs[dep.ToLinearID] {
					continue
				}
				filteredDeps = append(filteredDeps, dep)
			}
			allDeps = filteredDeps
		}
	}

	prefix, err := store.GetConfig(ctx, "issue_prefix")
	if err != nil || prefix == "" {
		prefix = "bd"
	}

	if idMode == "hash" {
		existingIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{IncludeTombstones: true})
		if err != nil {
			return stats, fmt.Errorf("failed to fetch existing issues for ID collision avoidance: %w", err)
		}
		usedIDs := make(map[string]bool, len(existingIssues))
		for _, issue := range existingIssues {
			if issue.ID != "" {
				usedIDs[issue.ID] = true
			}
		}

		idOpts := linear.IDGenerationOptions{
			BaseLength: hashLength,
			MaxLength:  8,
			UsedIDs:    usedIDs,
		}
		if err := linear.GenerateIssueIDs(beadsIssues, prefix, "linear-import", idOpts); err != nil {
			return stats, fmt.Errorf("failed to generate issue IDs: %w", err)
		}
	} else if idMode != "db" {
		return stats, fmt.Errorf("unsupported linear.id_mode %q (expected \"hash\" or \"db\")", idMode)
	}

	opts := ImportOptions{
		DryRun:     dryRun,
		SkipUpdate: false,
	}

	result, err := importIssuesCore(ctx, dbPath, store, beadsIssues, opts)
	if err != nil {
		return stats, fmt.Errorf("import failed: %w", err)
	}

	stats.Created = result.Created
	stats.Updated = result.Updated
	stats.Skipped = result.Skipped

	if dryRun {
		if stats.Incremental {
			fmt.Printf("  Would import %d issues from Linear (incremental since %s)\n",
				len(beadsIssues), stats.SyncedSince)
		} else {
			fmt.Printf("  Would import %d issues from Linear (full sync)\n", len(beadsIssues))
		}
		if verboseFlag {
			for _, issue := range beadsIssues {
				linearID := ""
				if issue.ExternalRef != nil {
					linearID = linear.ExtractLinearIdentifier(*issue.ExternalRef)
				}
				if linearID != "" {
					fmt.Printf("    %s\n", linearID)
				} else {
					fmt.Printf("    %s (no Linear ref)\n", issue.ID)
				}
			}
		}
		if len(canceledLinearIDs) > 0 {
			allIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
			if err == nil {
				canceledSet := make(map[string]bool, len(canceledLinearIDs))
				for _, id := range canceledLinearIDs {
					canceledSet[id] = true
				}
				var wouldClose []string
				for _, issue := range allIssues {
					if issue.ExternalRef == nil || !linear.IsLinearExternalRef(*issue.ExternalRef) || issue.Status == types.StatusClosed {
						continue
					}
					linearID := linear.ExtractLinearIdentifier(*issue.ExternalRef)
					if linearID != "" && canceledSet[linearID] {
						wouldClose = append(wouldClose, fmt.Sprintf("%s -> %s", issue.ID, linearID))
					}
				}
				if len(wouldClose) > 0 {
					fmt.Printf("  Would close %d local beads (Linear issues canceled)\n", len(wouldClose))
					if verboseFlag {
						for _, s := range wouldClose {
							fmt.Printf("    %s\n", s)
						}
					}
				}
			}
		}
		return stats, nil
	}

	allBeadsIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to fetch issues for dependency mapping: %v\n", err)
		return stats, nil
	}

	for _, issue := range allBeadsIssues {
		if issue.ExternalRef != nil && linear.IsLinearExternalRef(*issue.ExternalRef) {
			linearID := linear.ExtractLinearIdentifier(*issue.ExternalRef)
			if linearID != "" {
				linearIDToBeadsID[linearID] = issue.ID
			}
		}
	}

	depsCreated := 0
	for _, dep := range allDeps {
		fromID, fromOK := linearIDToBeadsID[dep.FromLinearID]
		toID, toOK := linearIDToBeadsID[dep.ToLinearID]

		if !fromOK || !toOK {
			continue
		}

		dependency := &types.Dependency{
			IssueID:     fromID,
			DependsOnID: toID,
			Type:        types.DependencyType(dep.Type),
			CreatedAt:   time.Now(),
		}
		err := store.AddDependency(ctx, dependency, actor)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") &&
				!strings.Contains(err.Error(), "duplicate") {
				fmt.Fprintf(os.Stderr, "Warning: failed to create dependency %s -> %s (%s): %v\n",
					fromID, toID, dep.Type, err)
			}
		} else {
			depsCreated++
		}
	}

	if depsCreated > 0 {
		fmt.Printf("  Created %d dependencies from Linear relations\n", depsCreated)
	}

	// Close local beads linked to Linear issues that were canceled
	if len(canceledLinearIDs) > 0 {
		canceledSet := make(map[string]bool, len(canceledLinearIDs))
		for _, id := range canceledLinearIDs {
			canceledSet[id] = true
		}
		closedCount := 0
		for _, issue := range allBeadsIssues {
			if issue.ExternalRef == nil || !linear.IsLinearExternalRef(*issue.ExternalRef) || issue.Status == types.StatusClosed {
				continue
			}
			linearID := linear.ExtractLinearIdentifier(*issue.ExternalRef)
			if linearID == "" || !canceledSet[linearID] {
				continue
			}
			now := time.Now()
			updates := map[string]interface{}{
				"status":   "closed",
				"closed_at": now,
			}
			if err := store.UpdateIssue(ctx, issue.ID, updates, actor); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to close bead %s (Linear %s canceled): %v\n", issue.ID, linearID, err)
			} else {
				closedCount++
			}
		}
		if closedCount > 0 {
			fmt.Printf("  Closed %d local beads (Linear issues canceled)\n", closedCount)
		}
	}

	return stats, nil
}

// doPushToLinear exports issues from beads to Linear using the GraphQL API.
//
// Push behavior:
//   - Beads with no external_ref are created in Linear (unless status is closed).
//   - Beads with a Linear external_ref are updated in Linear when local is newer.
//   - Closed beads are never created in Linear; this avoids re-creating issues after --fix
//     unlinks beads whose Linear issue was deleted.
//
// Deleted Linear issues: If a bead's external_ref points to a Linear issue that no longer
// exists (deleted in Linear), we warn and collect the bead ID. A summary is always printed
// when any such beads exist. With --fix (and not dry run), we clear external_ref and set
// status=closed for those beads so they do not sync again.
//
// Parameters:
//   - typeFilters: only push issues whose type is in this list (empty = all).
//   - excludeTypes: do not push issues whose type is in this list.
//   - includeEphemeral: if false (default), ephemeral/wisp/digest issues are excluded.
//   - fix: when true and not dry run, clear external_ref and close beads that point to
//     deleted Linear issues so they will not sync again.
func doPushToLinear(ctx context.Context, dryRun bool, createOnly bool, updateRefs bool, forceUpdateIDs map[string]bool, skipUpdateIDs map[string]bool, typeFilters []string, excludeTypes []string, includeEphemeral bool, fix bool) (*linear.PushStats, error) {
	stats := &linear.PushStats{}

	client, err := getLinearClient(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to create Linear client: %w", err)
	}

	filter := types.IssueFilter{}
	if !includeEphemeral {
		filter.Ephemeral = &includeEphemeral
	}
	allIssues, err := store.SearchIssues(ctx, "", filter)
	if err != nil {
		return stats, fmt.Errorf("failed to get local issues: %w", err)
	}

	// Apply type filters
	if len(typeFilters) > 0 || len(excludeTypes) > 0 {
		typeSet := make(map[string]bool, len(typeFilters))
		for _, t := range typeFilters {
			typeSet[strings.ToLower(t)] = true
		}
		excludeSet := make(map[string]bool, len(excludeTypes))
		for _, t := range excludeTypes {
			excludeSet[strings.ToLower(t)] = true
		}

		var filtered []*types.Issue
		for _, issue := range allIssues {
			issueType := strings.ToLower(string(issue.IssueType))

			// If type filters specified, issue must match one
			if len(typeFilters) > 0 && !typeSet[issueType] {
				continue
			}
			// If exclude types specified, issue must not match any
			if excludeSet[issueType] {
				continue
			}
			filtered = append(filtered, issue)
		}
		allIssues = filtered
	}

	var toCreate []*types.Issue
	var toUpdate []*types.Issue

	for _, issue := range allIssues {
		if issue.IsTombstone() {
			continue
		}
		// Exclude ephemeral/wisp/digest issues when includeEphemeral is false (belt-and-suspenders:
		// catches wisps and digest summaries even if DB ephemeral flag was not set)
		if !includeEphemeral {
			if issue.IsEffectivelyEphemeral() {
				continue
			}
			// Digest issues (mol squash summaries) and "Digest: ..." titles are ephemeral output
			if strings.ToLower(string(issue.IssueType)) == "digest" || strings.HasPrefix(issue.Title, "Digest: ") {
				continue
			}
		}

		if issue.ExternalRef != nil && linear.IsLinearExternalRef(*issue.ExternalRef) {
			if !createOnly {
				toUpdate = append(toUpdate, issue)
			}
		} else if issue.ExternalRef == nil {
			// Never create new Linear issues for closed beads (avoids re-pushing after --fix
			// clears ref and closes beads whose Linear issue was deleted).
			if issue.Status != types.StatusClosed {
				toCreate = append(toCreate, issue)
			}
		}
	}

	var stateCache *linear.StateCache
	if !dryRun && (len(toCreate) > 0 || (!createOnly && len(toUpdate) > 0)) {
		stateCache, err = linear.BuildStateCache(ctx, client)
		if err != nil {
			return stats, fmt.Errorf("failed to fetch team states: %w", err)
		}
	}

	mappingConfig := loadLinearMappingConfig(ctx)

	for _, issue := range toCreate {
		if dryRun {
			stats.Created++
			continue
		}

		linearPriority := linear.PriorityToLinear(issue.Priority, mappingConfig)
		stateID := stateCache.FindStateForBeadsStatus(issue.Status)

		description := linear.BuildLinearDescription(issue)

		linearIssue, err := client.CreateIssue(ctx, issue.Title, description, linearPriority, stateID, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to create issue '%s' in Linear: %v\n", issue.Title, err)
			stats.Errors++
			continue
		}

		stats.Created++
		fmt.Printf("  Created: %s -> %s\n", issue.ID, linearIssue.Identifier)

		if updateRefs && linearIssue.URL != "" {
			externalRef := linearIssue.URL
			if canonical, ok := linear.CanonicalizeLinearExternalRef(externalRef); ok {
				externalRef = canonical
			}
			updates := map[string]interface{}{
				"external_ref": externalRef,
			}
			if err := store.UpdateIssue(ctx, issue.ID, updates, actor); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to update external_ref for %s: %v\n", issue.ID, err)
				stats.Errors++
			}
		}
	}

	var wouldUpdateIDs []string
	// Beads whose external_ref points to a Linear issue that no longer exists (deleted).
	// We warn per-issue, then print a summary and optionally apply --fix (clear ref + close).
	var deletedLinearRefBeadIDs []string
	if len(toUpdate) > 0 && !createOnly {
		for _, issue := range toUpdate {
			if skipUpdateIDs != nil && skipUpdateIDs[issue.ID] {
				stats.Skipped++
				continue
			}

			linearIdentifier := linear.ExtractLinearIdentifier(*issue.ExternalRef)
			if linearIdentifier == "" {
				fmt.Fprintf(os.Stderr, "Warning: could not extract Linear identifier from %s: %s\n",
					issue.ID, *issue.ExternalRef)
				stats.Errors++
				continue
			}

			linearIssue, err := client.FetchIssueByIdentifier(ctx, linearIdentifier)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to fetch Linear issue %s: %v\n",
					linearIdentifier, err)
				stats.Errors++
				continue
			}
			if linearIssue == nil {
				// Linear issue was deleted; record so we can print summary and optionally --fix.
				fmt.Fprintf(os.Stderr, "Warning: Linear issue %s not found (may have been deleted)\n",
					linearIdentifier)
				deletedLinearRefBeadIDs = append(deletedLinearRefBeadIDs, issue.ID)
				stats.Skipped++
				continue
			}

			linearUpdatedAt, err := time.Parse(time.RFC3339, linearIssue.UpdatedAt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to parse Linear UpdatedAt for %s: %v\n",
					linearIdentifier, err)
				stats.Errors++
				continue
			}

			forcedUpdate := forceUpdateIDs != nil && forceUpdateIDs[issue.ID]
			if !forcedUpdate && !issue.UpdatedAt.After(linearUpdatedAt) {
				stats.Skipped++
				continue
			}

			if !forcedUpdate {
				localComparable := linear.NormalizeIssueForLinearHash(issue)
				linearComparable := linear.IssueToBeads(linearIssue, mappingConfig).Issue.(*types.Issue)
				if localComparable.ComputeContentHash() == linearComparable.ComputeContentHash() {
					stats.Skipped++
					continue
				}
			}

			if dryRun {
				stats.Updated++
				wouldUpdateIDs = append(wouldUpdateIDs, fmt.Sprintf("%s -> %s", issue.ID, linearIdentifier))
				continue
			}

			description := linear.BuildLinearDescription(issue)

			updatePayload := map[string]interface{}{
				"title":       issue.Title,
				"description": description,
			}

			linearPriority := linear.PriorityToLinear(issue.Priority, mappingConfig)
			if linearPriority > 0 {
				updatePayload["priority"] = linearPriority
			}

			stateID := stateCache.FindStateForBeadsStatus(issue.Status)
			if stateID != "" {
				updatePayload["stateId"] = stateID
			}

			updatedLinearIssue, err := client.UpdateIssue(ctx, linearIssue.ID, updatePayload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to update Linear issue %s: %v\n",
					linearIdentifier, err)
				stats.Errors++
				continue
			}

			stats.Updated++
			fmt.Printf("  Updated: %s -> %s\n", issue.ID, updatedLinearIssue.Identifier)
		}
	}

	if dryRun {
		fmt.Printf("  Would create %d issues in Linear\n", stats.Created)
		if verboseFlag {
			for _, issue := range toCreate {
				fmt.Printf("    %s\n", issue.ID)
			}
		}
		if !createOnly {
			fmt.Printf("  Would update %d issues in Linear\n", stats.Updated)
			if verboseFlag {
				for _, s := range wouldUpdateIDs {
					fmt.Printf("    %s\n", s)
				}
			}
		}
	}

	// Deleted-ref summary: print whenever we hit any "Linear issue not found" warnings,
	// so the user always sees the hint to use --fix (dry run or not).
	if len(deletedLinearRefBeadIDs) > 0 {
		fmt.Printf("  %d beads point to deleted Linear issues; use --fix to clear external_ref and close them so they do not sync again:\n", len(deletedLinearRefBeadIDs))
		if verboseFlag {
			for _, id := range deletedLinearRefBeadIDs {
				fmt.Printf("    %s\n", id)
			}
		}
	}

	// Apply --fix: clear external_ref and close beads that pointed to deleted Linear issues.
	// Only when fix is true and not dry run. Closing prevents them from being re-pushed
	// (closed beads are excluded from the "to create" list).
	if !dryRun && fix && len(deletedLinearRefBeadIDs) > 0 {
		cleared := 0
		now := time.Now()
		for _, id := range deletedLinearRefBeadIDs {
			updates := map[string]interface{}{
				"external_ref": nil,
				"status":       string(types.StatusClosed),
				"closed_at":    now,
			}
			if err := store.UpdateIssue(ctx, id, updates, actor); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to fix bead %s (clear ref + close): %v\n", id, err)
			} else {
				cleared++
			}
		}
		if cleared > 0 {
			fmt.Printf("  Cleared external_ref and closed %d beads (Linear issues deleted; they will not sync again)\n", cleared)
		}
	}

	return stats, nil
}

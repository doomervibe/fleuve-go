// Package uibackend implements the Fleuve UI read API (HTTP JSON) expected by the
// vendored React app—mirroring Python's fleuve.ui.backend.api.FleuveUIBackend.
//
// It is a library: the host supplies a pgx pool and optional table overrides.
// Optional [Options.StateResolver] and [Options.Replay] (per workflow_type) supply
// typed workflow state for list/detail; otherwise state matches Python's degraded
// mode (JSON from the latest event body). Return [ErrStateUnresolved] from
// StateResolver to fall through to Replay or latest-event behavior.
//
// Static assets and SPA routing live in package uiembed; compose both in your
// application's main or use [NewCombinedHandler] for a reference stack.
//
// Batch cancel/replay return HTTP 501 with the same messages as Python unless
// you wire a custom gateway later.
package uibackend

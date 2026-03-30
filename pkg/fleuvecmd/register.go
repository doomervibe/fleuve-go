// Package fleuvecmd wires built-in workflow types for fleuve-gateway and fleuve-runner.
package fleuvecmd

import (
	"os"

	"github.com/fleuve/fleuve-go/pkg/actions"
	"github.com/fleuve/fleuve-go/pkg/config"
	"github.com/fleuve/fleuve-go/pkg/counterworkflow"
	"github.com/fleuve/fleuve-go/pkg/gateway"
	"github.com/fleuve/fleuve-go/pkg/model"
	"github.com/fleuve/fleuve-go/pkg/repo"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewCounterRepo builds the default PGX repository for CounterWorkflow.
func NewCounterRepo(pool *pgxpool.Pool, cfg *config.Config, jet repo.JetStreamCommittedPublisher) (*counterworkflow.Workflow, *repo.PGXRepo) {
	wf := counterworkflow.New()
	es := repo.NewInProcessEphemeralStorage(cfg.MaxCacheSize)
	opts := []repo.PGXRepoOption{repo.WithPGXAdapter(actions.NoopModelAdapter())}
	if cfg.SnapshotInterval > 0 {
		opts = append(opts, repo.WithPGXSnapshotInterval(cfg.SnapshotInterval))
	}
	if jet != nil {
		opts = append(opts, repo.WithPGXJetStream(jet))
	}
	r := repo.NewPGXRepo(pool, wf.Name(), wf, es, opts...)
	return wf, r
}

// WireCounterGateway registers CounterWorkflow on the gateway and returns the shared ActionExecutor (already configured for DB-backed activities).
// If jet is non-nil, committed events are published to JetStream after each successful DB commit (NATS runners).
func WireCounterGateway(gw *gateway.FleuveCommandGateway, pool *pgxpool.Pool, cfg *config.Config, jet repo.JetStreamCommittedPublisher) *actions.ActionExecutor {
	wf, r := NewCounterRepo(pool, cfg, jet)
	ae := actions.NewActionExecutor(actions.NoopModelAdapter(), r,
		actions.WithRunnerName(RunnerName()),
		actions.WithActivityPersistence(pool, wf),
	)
	gw.RegisterWorkflowType(wf.Name(), r, counterworkflow.ParseGatewayCommand)
	gw.RegisterWorkflowModel(wf.Name(), wf)
	gw.SetActionExecutor(ae)
	return ae
}

// CounterWorkflow returns the built-in counter aggregate.
func CounterWorkflow() *counterworkflow.Workflow {
	return counterworkflow.New()
}

// RunnerName returns FLEUVE_RUNNER_NAME or the host name for activity bookkeeping.
func RunnerName() string {
	if v := os.Getenv("FLEUVE_RUNNER_NAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}

// Ensure model.Workflow is satisfied (compile-time).
var _ model.Workflow = (*counterworkflow.Workflow)(nil)

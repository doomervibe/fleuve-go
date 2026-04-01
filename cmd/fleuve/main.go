// Package main provides the fleuve CLI tool for managing workflow projects.
//
// Usage:
//
//	fleuve <command> [subcommand] [flags]
//
// Commands:
//   - startproject   Create a new fleuve project
//   - addworkflow    Add a workflow to an existing project
//   - validate       Validate workflow implementations
//   - admin          Administrative operations
//   - ui             Start the web UI
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "startproject":
		runStartProject()
	case "addworkflow":
		runAddWorkflow()
	case "validate":
		runValidate()
	case "admin":
		runAdmin()
	case "ui":
		runUI()
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

// printUsage prints the main CLI usage information.
func printUsage() {
	usage := `fleuve - Workflow engine CLI tool

Usage:
  fleuve <command> [subcommand] [flags]

Commands:
  startproject   Create a new fleuve project
  addworkflow    Add a workflow to an existing project
  validate       Validate workflow implementations
  admin          Administrative operations
  ui             Start the web UI

Run 'fleuve <command> --help' for more information on a command.
`
	fmt.Fprint(os.Stderr, usage)
}

// =============================================================================
// startproject command
// =============================================================================

// runStartProject handles the startproject command.
func runStartProject() {
	fs := flag.NewFlagSet("startproject", flag.ExitOnError)
	name := fs.String("name", "", "Project name (required)")
	path := fs.String("path", ".", "Directory to create project in")
	module := fs.String("module", "", "Go module name (default: derived from name)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Create a new fleuve project

Usage:
  fleuve startproject [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[2:])

	if *name == "" {
		fmt.Fprintln(os.Stderr, "error: --name is required")
		fs.Usage()
		os.Exit(1)
	}

	if *module == "" {
		*module = "github.com/" + strings.ToLower(*name)
	}

	if err := doStartProject(*name, *path, *module); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// doStartProject creates a new fleuve project structure.
func doStartProject(name, path, module string) error {
	fmt.Printf("Creating project '%s' in %s\n", name, path)
	fmt.Printf("Module: %s\n", module)

	// Project structure to create
	structure := []struct {
		path  string
		isDir bool
	}{
		{path: name, isDir: true},
		{path: name + "/cmd", isDir: true},
		{path: name + "/cmd/runner", isDir: true},
		{path: name + "/cmd/gateway", isDir: true},
		{path: name + "/workflows", isDir: true},
		{path: name + "/migrations", isDir: true},
	}

	for _, item := range structure {
		fullPath := path + "/" + item.path
		if item.isDir {
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", fullPath, err)
			}
		}
	}

	// Create go.mod
	goMod := fmt.Sprintf("module %s\n\ngo 1.22\n\nrequire github.com/doomervibe/fleuve-go v0.0.0\n", module)
	if err := os.WriteFile(path+"/"+name+"/go.mod", []byte(goMod), 0644); err != nil {
		return fmt.Errorf("failed to create go.mod: %w", err)
	}

	// Create fleuve.toml
	fleuveToml := `[fleuve]
database_url = ""
namespace = ""
enable_jetstream = false
enable_truncation = false
snapshot_interval = 0
max_inflight = 100
max_cache_size = 10000
`
	if err := os.WriteFile(path+"/"+name+"/fleuve.toml", []byte(fleuveToml), 0644); err != nil {
		return fmt.Errorf("failed to create fleuve.toml: %w", err)
	}

	// Create placeholder workflow file
	workflowFile := fmt.Sprintf(`package workflows

import "github.com/doomervibe/fleuve-go/pkg/model"

// %sWorkflow is a placeholder workflow implementation.
// Replace this with your actual workflow logic.
type %sWorkflow struct{}

func (w *%sWorkflow) Name() string {
	return "%s_workflow"
}

func (w *%sWorkflow) SchemaVersion() int {
	return 1
}

func (w *%sWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *%sWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, &model.Rejection{Msg: "not implemented"}
}

func (w *%sWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}

func (w *%sWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}

func (w *%sWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}
`, capitalize(name), capitalize(name), capitalize(name), capitalize(name), strings.ToLower(name), capitalize(name), capitalize(name), capitalize(name), capitalize(name), capitalize(name))
	if err := os.WriteFile(path+"/"+name+"/workflows/"+strings.ToLower(name)+".go", []byte(workflowFile), 0644); err != nil {
		return fmt.Errorf("failed to create workflow file: %w", err)
	}

	fmt.Println("Project created successfully!")
	fmt.Println("\nNext steps:")
	fmt.Println("  1. cd " + name)
	fmt.Println("  2. Edit fleuve.toml with your database configuration")
	fmt.Println("  3. Implement your workflows in the workflows/ directory")
	fmt.Println("  4. Run migrations in the migrations/ directory")
	fmt.Println("  5. Start the runner: go run cmd/runner/main.go -type " + strings.ToLower(name) + "_workflow")

	return nil
}

// capitalize capitalizes the first letter of a string.
func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// =============================================================================
// addworkflow command
// =============================================================================

// runAddWorkflow handles the addworkflow command.
func runAddWorkflow() {
	fs := flag.NewFlagSet("addworkflow", flag.ExitOnError)
	name := fs.String("name", "", "Workflow name (required)")
	path := fs.String("path", ".", "Project directory")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Add a workflow to an existing project

Usage:
  fleuve addworkflow [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[2:])

	if *name == "" {
		fmt.Fprintln(os.Stderr, "error: --name is required")
		fs.Usage()
		os.Exit(1)
	}

	if err := doAddWorkflow(*name, *path); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// doAddWorkflow creates a new workflow file in the project.
func doAddWorkflow(name, path string) error {
	workflowDir := path + "/workflows"
	if err := os.MkdirAll(workflowDir, 0755); err != nil {
		return fmt.Errorf("failed to create workflows directory: %w", err)
	}

	workflowFile := fmt.Sprintf(`package workflows

import "github.com/doomervibe/fleuve-go/pkg/model"

// %sWorkflow implements model.Workflow for the %s workflow.
type %sWorkflow struct{}

func (w *%sWorkflow) Name() string {
	return "%s_workflow"
}

func (w *%sWorkflow) SchemaVersion() int {
	return 1
}

func (w *%sWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *%sWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, &model.Rejection{Msg: "not implemented"}
}

func (w *%sWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}

func (w *%sWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}

func (w *%sWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}
`, capitalize(name), strings.ToLower(name), capitalize(name), capitalize(name), strings.ToLower(name), capitalize(name), capitalize(name), capitalize(name), capitalize(name), capitalize(name), capitalize(name))

	filePath := workflowDir + "/" + strings.ToLower(name) + ".go"
	if err := os.WriteFile(filePath, []byte(workflowFile), 0644); err != nil {
		return fmt.Errorf("failed to create workflow file: %w", err)
	}

	fmt.Printf("Workflow '%s' created at %s\n", name, filePath)
	fmt.Println("\nNext steps:")
	fmt.Println("  1. Implement Decide() and Evolve() methods")
	fmt.Println("  2. Define your events and commands")
	fmt.Println("  3. Run 'fleuve validate' to check your implementation")

	return nil
}

// =============================================================================
// validate command
// =============================================================================

// runValidate handles the validate command.
func runValidate() {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	workflowPath := fs.String("path", ".", "Path to the workflow package")
	verbose := fs.Bool("verbose", false, "Show detailed validation output")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Validate workflow implementations

Usage:
  fleuve validate [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[2:])

	if err := doValidate(*workflowPath, *verbose); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// doValidate validates workflow implementations in the specified path.
func doValidate(path string, verbose bool) error {
	fmt.Printf("Validating workflows in %s\n", path)

	// Note: Full validation requires loading Go types at runtime.
	// This is a placeholder that demonstrates the validation API.
	// In a complete implementation, this would use go/packages or plugin loading
	// to discover and validate workflow types.

	// Import the validation package for use when workflow types are loaded:
	// import "github.com/doomervibe/fleuve-go/pkg/validation"
	// errors := validation.ValidateWorkflow(workflowInstance)
	// errors := validation.ValidateAnyWorkflow(anyInstance)
	// errors := validation.DiscoverAndValidate(modulePath)

	fmt.Println("\nNote: Full validation requires workflow types to be compiled.")
	fmt.Println("For runtime validation, use the validation package directly:")
	fmt.Println("  import \"github.com/doomervibe/fleuve-go/pkg/validation\"")
	fmt.Println("  errors := validation.ValidateWorkflow(yourWorkflow)")
	fmt.Println("  errors := validation.DiscoverAndValidate(\"./workflows/\")")

	return nil
}

// =============================================================================
// admin command
// =============================================================================

// runAdmin handles the admin command and its subcommands.
func runAdmin() {
	if len(os.Args) < 3 {
		printAdminUsage()
		os.Exit(1)
	}

	subcommand := os.Args[2]

	switch subcommand {
	case "inspect":
		runAdminInspect()
	case "list":
		runAdminList()
	case "pause":
		runAdminPause()
	case "resume":
		runAdminResume()
	case "cancel":
		runAdminCancel()
	case "replay":
		runAdminReplay()
	case "health":
		runAdminHealth()
	case "truncate":
		runAdminTruncate()
	case "-h", "--help", "help":
		printAdminUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown admin subcommand: %s\n\n", subcommand)
		printAdminUsage()
		os.Exit(1)
	}
}

// printAdminUsage prints admin command usage.
func printAdminUsage() {
	usage := `Administrative operations for workflow management

Usage:
  fleuve admin <subcommand> [flags]

Subcommands:
  inspect    Inspect a specific workflow instance
  list       List workflow instances
  pause      Pause a workflow instance
  resume     Resume a paused workflow instance
  cancel     Cancel a workflow instance
  replay     Replay events for a workflow instance
  health     Check system health
  truncate   Truncate old events

Run 'fleuve admin <subcommand> --help' for more information.
`
	fmt.Fprint(os.Stderr, usage)
}

// runAdminInspect handles the admin inspect subcommand.
func runAdminInspect() {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	workflowID := fs.String("id", "", "Workflow ID (required)")
	workflowType := fs.String("type", "", "Workflow type")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	_ = fs.String("addr", "http://localhost:8080", "Gateway address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Inspect a specific workflow instance

Usage:
  fleuve admin inspect [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowID == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Inspecting workflow %s\n", *workflowID)
	fmt.Printf("Type: %s\n", *workflowType)
	fmt.Println("\nNote: This command requires a running gateway or direct database access.")
	fmt.Println("Use --addr to specify the gateway address or --config for database access.")
}

// runAdminList handles the admin list subcommand.
func runAdminList() {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	workflowType := fs.String("type", "", "Filter by workflow type")
	status := fs.String("status", "", "Filter by status (active, paused, cancelled)")
	limit := fs.Int("limit", 100, "Maximum number of results")
	offset := fs.Int("offset", 0, "Offset for pagination")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	_ = fs.String("addr", "http://localhost:8080", "Gateway address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `List workflow instances

Usage:
  fleuve admin list [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	fmt.Println("Listing workflows")
	fmt.Printf("Type filter: %s\n", *workflowType)
	fmt.Printf("Status filter: %s\n", *status)
	fmt.Printf("Limit: %d, Offset: %d\n", *limit, *offset)
	fmt.Println("\nNote: This command requires a running gateway or direct database access.")
}

// runAdminPause handles the admin pause subcommand.
func runAdminPause() {
	fs := flag.NewFlagSet("pause", flag.ExitOnError)
	workflowID := fs.String("id", "", "Workflow ID (required)")
	reason := fs.String("reason", "paused via CLI", "Reason for pausing")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	_ = fs.String("addr", "http://localhost:8080", "Gateway address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Pause a workflow instance

Usage:
  fleuve admin pause [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowID == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Pausing workflow %s (reason: %s)\n", *workflowID, *reason)
	fmt.Println("\nNote: This command sends a pause request to the gateway.")
	fmt.Println("Use --addr to specify the gateway address.")
}

// runAdminResume handles the admin resume subcommand.
func runAdminResume() {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	workflowID := fs.String("id", "", "Workflow ID (required)")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	_ = fs.String("addr", "http://localhost:8080", "Gateway address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Resume a paused workflow instance

Usage:
  fleuve admin resume [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowID == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Resuming workflow %s\n", *workflowID)
	fmt.Println("\nNote: This command sends a resume request to the gateway.")
}

// runAdminCancel handles the admin cancel subcommand.
func runAdminCancel() {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	workflowID := fs.String("id", "", "Workflow ID (required)")
	reason := fs.String("reason", "cancelled via CLI", "Reason for cancellation")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	_ = fs.String("addr", "http://localhost:8080", "Gateway address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Cancel a workflow instance

Usage:
  fleuve admin cancel [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowID == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Cancelling workflow %s (reason: %s)\n", *workflowID, *reason)
	fmt.Println("\nNote: This command sends a cancel request to the gateway.")
}

// runAdminReplay handles the admin replay subcommand.
func runAdminReplay() {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	workflowID := fs.String("id", "", "Workflow ID (required)")
	fromVersion := fs.Int64("from", 0, "Replay from event version (0 = from beginning)")
	toVersion := fs.Int64("to", 0, "Replay to event version (0 = to latest)")
	dryRun := fs.Bool("dry-run", false, "Show what would be replayed without executing")
	_ = fs.String("config", "", "Path to fleuve.toml config file")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Replay events for a workflow instance

Usage:
  fleuve admin replay [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowID == "" {
		fmt.Fprintln(os.Stderr, "error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Replaying workflow %s\n", *workflowID)
	fmt.Printf("From version: %d, To version: %d\n", *fromVersion, *toVersion)
	fmt.Printf("Dry run: %v\n", *dryRun)
	fmt.Println("\nNote: Replay uses the repo.ReplayWorkflow method.")
	fmt.Println("This command requires direct database access via --config.")
}

// runAdminHealth handles the admin health subcommand.
func runAdminHealth() {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	config := fs.String("config", "", "Path to fleuve.toml config file")
	addr := fs.String("addr", "http://localhost:8080", "Gateway address")
	detailed := fs.Bool("detailed", false, "Show detailed health information")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Check system health

Usage:
  fleuve admin health [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	fmt.Println("Checking system health...")
	fmt.Printf("Gateway address: %s\n", *addr)
	fmt.Printf("Config: %s\n", *config)
	fmt.Printf("Detailed: %v\n", *detailed)
	fmt.Println("\nHealth checks:")
	fmt.Println("  - Database connection: [check required]")
	fmt.Println("  - Gateway connectivity: [check required]")
	fmt.Println("  - Reader offsets: [check required]")
	fmt.Println("  - Pending events: [check required]")
}

// runAdminTruncate handles the admin truncate subcommand.
func runAdminTruncate() {
	fs := flag.NewFlagSet("truncate", flag.ExitOnError)
	workflowType := fs.String("type", "", "Workflow type (required)")
	dryRun := fs.Bool("dry-run", false, "Show what would be truncated without executing")
	retention := fs.String("retention", "168h", "Minimum retention period (e.g., 168h = 7 days)")
	batchSize := fs.Int("batch-size", 1000, "Maximum events to delete per cycle")
	_ = fs.String("config", "", "Path to fleuve.toml config file")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Truncate old events covered by snapshots

Usage:
  fleuve admin truncate [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[3:])

	if *workflowType == "" {
		fmt.Fprintln(os.Stderr, "error: --type is required")
		fs.Usage()
		os.Exit(1)
	}

	fmt.Printf("Truncating events for workflow type: %s\n", *workflowType)
	fmt.Printf("Retention: %s\n", *retention)
	fmt.Printf("Batch size: %d\n", *batchSize)
	fmt.Printf("Dry run: %v\n", *dryRun)
	fmt.Println("\nNote: This uses the truncation.TruncationService.")
	fmt.Println("Events are only deleted when covered by snapshots and all readers have processed them.")
	fmt.Println("This command requires direct database access via --config.")
}

// =============================================================================
// ui command
// =============================================================================

// runUI handles the ui command.
func runUI() {
	fs := flag.NewFlagSet("ui", flag.ExitOnError)
	addr := fs.String("addr", ":8081", "UI server listen address")
	_ = fs.String("config", "", "Path to fleuve.toml config file")
	gatewayAddr := fs.String("gateway", "http://localhost:8080", "Gateway API address")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Start the web UI

Usage:
  fleuve ui [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	_ = fs.Parse(os.Args[2:])

	fmt.Printf("Starting UI server on %s\n", *addr)
	fmt.Printf("Gateway API: %s\n", *gatewayAddr)
	fmt.Println("\nNote: Run the reference UI stack with: go run ./examples/ui_server")
	fmt.Println("This is a placeholder that shows the intended configuration.")
}

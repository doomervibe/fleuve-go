package validation

import (
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// =============================================================================
// Valid Workflow Implementation (for positive test cases)
// =============================================================================

type validState struct {
	*model.StateBase
	Data string `json:"data"`
}

func (s *validState) Copy() model.State {
	return &validState{
		StateBase: s.StateBase.Copy().(*model.StateBase),
		Data:      s.Data,
	}
}

type validCommand struct {
	action string
}

func (c *validCommand) CommandType() string { return "valid_command" }

type validEvent struct {
	model.EventBase
	Value string `json:"value"`
}

func (e *validEvent) Type() string { return "valid_event" }

type validWorkflow struct{}

func (w *validWorkflow) Name() string { return "valid_workflow" }
func (w *validWorkflow) SchemaVersion() int {
	return 1
}
func (w *validWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}
func (w *validWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *validWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *validWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}
func (w *validWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}

// =============================================================================
// Invalid Workflow Implementations (for negative test cases)
// =============================================================================

// emptyNameWorkflow has an empty Name()
type emptyNameWorkflow struct {
	validWorkflow
}

func (w *emptyNameWorkflow) Name() string { return "" }

// whitespaceNameWorkflow has whitespace in Name()
type whitespaceNameWorkflow struct {
	validWorkflow
}

func (w *whitespaceNameWorkflow) Name() string { return "  padded  " }

// zeroSchemaVersionWorkflow has SchemaVersion() = 0
type zeroSchemaVersionWorkflow struct {
	validWorkflow
}

func (w *zeroSchemaVersionWorkflow) SchemaVersion() int { return 0 }

// negativeSchemaVersionWorkflow has SchemaVersion() < 0
type negativeSchemaVersionWorkflow struct {
	validWorkflow
}

func (w *negativeSchemaVersionWorkflow) SchemaVersion() int { return -1 }

// missingDecideWorkflow doesn't have Decide method with correct signature
type missingDecideWorkflow struct{}

func (w *missingDecideWorkflow) Name() string       { return "missing_decide" }
func (w *missingDecideWorkflow) SchemaVersion() int { return 1 }
func (w *missingDecideWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *missingDecideWorkflow) Decide() {} // Wrong signature - no params
func (w *missingDecideWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *missingDecideWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *missingDecideWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// wrongDecideParamsWorkflow has wrong Decide parameters
type wrongDecideParamsWorkflow struct{}

func (w *wrongDecideParamsWorkflow) Name() string       { return "wrong_decide_params" }
func (w *wrongDecideParamsWorkflow) SchemaVersion() int { return 1 }
func (w *wrongDecideParamsWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongDecideParamsWorkflow) Decide(state string, cmd int) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongDecideParamsWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongDecideParamsWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongDecideParamsWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// wrongDecideReturnsWorkflow has wrong Decide return types
type wrongDecideReturnsWorkflow struct{}

func (w *wrongDecideReturnsWorkflow) Name() string       { return "wrong_decide_returns" }
func (w *wrongDecideReturnsWorkflow) SchemaVersion() int { return 1 }
func (w *wrongDecideReturnsWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongDecideReturnsWorkflow) Decide(state model.State, cmd model.Command) (string, error) {
	return "", nil
}
func (w *wrongDecideReturnsWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongDecideReturnsWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongDecideReturnsWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// missingEvolveWorkflow doesn't have Evolve method with correct signature
type missingEvolveWorkflow struct{}

func (w *missingEvolveWorkflow) Name() string       { return "missing_evolve" }
func (w *missingEvolveWorkflow) SchemaVersion() int { return 1 }
func (w *missingEvolveWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *missingEvolveWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *missingEvolveWorkflow) Evolve()                                {} // Wrong signature
func (w *missingEvolveWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *missingEvolveWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// wrongEvolveParamsWorkflow has wrong Evolve parameters
type wrongEvolveParamsWorkflow struct{}

func (w *wrongEvolveParamsWorkflow) Name() string       { return "wrong_evolve_params" }
func (w *wrongEvolveParamsWorkflow) SchemaVersion() int { return 1 }
func (w *wrongEvolveParamsWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongEvolveParamsWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongEvolveParamsWorkflow) Evolve(state string, event string) model.State {
	return nil
}
func (w *wrongEvolveParamsWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongEvolveParamsWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// wrongEvolveReturnWorkflow has wrong Evolve return type
type wrongEvolveReturnWorkflow struct{}

func (w *wrongEvolveReturnWorkflow) Name() string       { return "wrong_evolve_return" }
func (w *wrongEvolveReturnWorkflow) SchemaVersion() int { return 1 }
func (w *wrongEvolveReturnWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongEvolveReturnWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongEvolveReturnWorkflow) Evolve(state model.State, event model.Event) string {
	return ""
}
func (w *wrongEvolveReturnWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongEvolveReturnWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// missingEventToCmdWorkflow doesn't have EventToCmd method with correct signature
type missingEventToCmdWorkflow struct{}

func (w *missingEventToCmdWorkflow) Name() string       { return "missing_event_to_cmd" }
func (w *missingEventToCmdWorkflow) SchemaVersion() int { return 1 }
func (w *missingEventToCmdWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *missingEventToCmdWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *missingEventToCmdWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *missingEventToCmdWorkflow) EventToCmd()                     {} // Wrong signature
func (w *missingEventToCmdWorkflow) IsFinalEvent(e model.Event) bool { return false }

// wrongEventToCmdParamsWorkflow has wrong EventToCmd parameters
type wrongEventToCmdParamsWorkflow struct{}

func (w *wrongEventToCmdParamsWorkflow) Name() string       { return "wrong_event_to_cmd_params" }
func (w *wrongEventToCmdParamsWorkflow) SchemaVersion() int { return 1 }
func (w *wrongEventToCmdParamsWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongEventToCmdParamsWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongEventToCmdParamsWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongEventToCmdParamsWorkflow) EventToCmd(e string) model.Command { return nil }
func (w *wrongEventToCmdParamsWorkflow) IsFinalEvent(e model.Event) bool   { return false }

// wrongEventToCmdReturnWorkflow has wrong EventToCmd return type
type wrongEventToCmdReturnWorkflow struct{}

func (w *wrongEventToCmdReturnWorkflow) Name() string       { return "wrong_event_to_cmd_return" }
func (w *wrongEventToCmdReturnWorkflow) SchemaVersion() int { return 1 }
func (w *wrongEventToCmdReturnWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongEventToCmdReturnWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongEventToCmdReturnWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongEventToCmdReturnWorkflow) EventToCmd(e model.Event) string { return "" }
func (w *wrongEventToCmdReturnWorkflow) IsFinalEvent(e model.Event) bool { return false }

// missingIsFinalEventWorkflow doesn't have IsFinalEvent method with correct signature
type missingIsFinalEventWorkflow struct{}

func (w *missingIsFinalEventWorkflow) Name() string       { return "missing_is_final_event" }
func (w *missingIsFinalEventWorkflow) SchemaVersion() int { return 1 }
func (w *missingIsFinalEventWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *missingIsFinalEventWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *missingIsFinalEventWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *missingIsFinalEventWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *missingIsFinalEventWorkflow) IsFinalEvent()                          {} // Wrong signature

// wrongIsFinalEventParamsWorkflow has wrong IsFinalEvent parameters
type wrongIsFinalEventParamsWorkflow struct{}

func (w *wrongIsFinalEventParamsWorkflow) Name() string       { return "wrong_is_final_event_params" }
func (w *wrongIsFinalEventParamsWorkflow) SchemaVersion() int { return 1 }
func (w *wrongIsFinalEventParamsWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongIsFinalEventParamsWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongIsFinalEventParamsWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongIsFinalEventParamsWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongIsFinalEventParamsWorkflow) IsFinalEvent(e string) bool             { return false }

// wrongIsFinalEventReturnWorkflow has wrong IsFinalEvent return type
type wrongIsFinalEventReturnWorkflow struct{}

func (w *wrongIsFinalEventReturnWorkflow) Name() string       { return "wrong_is_final_event_return" }
func (w *wrongIsFinalEventReturnWorkflow) SchemaVersion() int { return 1 }
func (w *wrongIsFinalEventReturnWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
func (w *wrongIsFinalEventReturnWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongIsFinalEventReturnWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongIsFinalEventReturnWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongIsFinalEventReturnWorkflow) IsFinalEvent(e model.Event) string      { return "" }

// missingUpcastWorkflow doesn't have Upcast method
type missingUpcastWorkflow struct{}

func (w *missingUpcastWorkflow) Name() string       { return "missing_upcast" }
func (w *missingUpcastWorkflow) SchemaVersion() int { return 1 }
func (w *missingUpcastWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *missingUpcastWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *missingUpcastWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *missingUpcastWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// wrongUpcastSignatureWorkflow has wrong Upcast signature
type wrongUpcastSignatureWorkflow struct{}

func (w *wrongUpcastSignatureWorkflow) Name() string        { return "wrong_upcast" }
func (w *wrongUpcastSignatureWorkflow) SchemaVersion() int  { return 1 }
func (w *wrongUpcastSignatureWorkflow) Upcast(x int) string { return "" } // Wrong signature
func (w *wrongUpcastSignatureWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (w *wrongUpcastSignatureWorkflow) Evolve(state model.State, event model.Event) model.State {
	return state
}
func (w *wrongUpcastSignatureWorkflow) EventToCmd(e model.Event) model.Command { return nil }
func (w *wrongUpcastSignatureWorkflow) IsFinalEvent(e model.Event) bool        { return false }

// multipleErrorsWorkflow has multiple validation errors
type multipleErrorsWorkflow struct{}

func (w *multipleErrorsWorkflow) Name() string       { return "" }
func (w *multipleErrorsWorkflow) SchemaVersion() int { return 0 }
func (w *multipleErrorsWorkflow) Upcast()            {} // Wrong signature
func (w *multipleErrorsWorkflow) Decide()            {} // Wrong signature
func (w *multipleErrorsWorkflow) Evolve()            {} // Wrong signature
func (w *multipleErrorsWorkflow) EventToCmd()        {} // Wrong signature
func (w *multipleErrorsWorkflow) IsFinalEvent()      {} // Wrong signature

// =============================================================================
// Tests for ValidateWorkflow (typed - requires model.Workflow interface)
// =============================================================================

func TestValidateWorkflow_ValidWorkflow(t *testing.T) {
	wf := &validWorkflow{}
	errors := ValidateWorkflow(wf)

	if len(errors) != 0 {
		t.Errorf("expected no errors for valid workflow, got: %v", errors)
	}
}

func TestValidateWorkflow_EmptyName(t *testing.T) {
	wf := &emptyNameWorkflow{}
	errors := ValidateWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Name()") && contains(err, "non-empty") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about empty Name(), got: %v", errors)
	}
}

func TestValidateWorkflow_WhitespaceName(t *testing.T) {
	wf := &whitespaceNameWorkflow{}
	errors := ValidateWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Name()") && contains(err, "whitespace") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about whitespace in Name(), got: %v", errors)
	}
}

func TestValidateWorkflow_ZeroSchemaVersion(t *testing.T) {
	wf := &zeroSchemaVersionWorkflow{}
	errors := ValidateWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "SchemaVersion()") && contains(err, "positive") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about zero SchemaVersion(), got: %v", errors)
	}
}

func TestValidateWorkflow_NegativeSchemaVersion(t *testing.T) {
	wf := &negativeSchemaVersionWorkflow{}
	errors := ValidateWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "SchemaVersion()") && contains(err, "positive") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about negative SchemaVersion(), got: %v", errors)
	}
}

// =============================================================================
// Tests for ValidateAnyWorkflow (untyped - uses reflection)
// =============================================================================

func TestValidateAnyWorkflow_ValidWorkflow(t *testing.T) {
	wf := &validWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	if len(errors) != 0 {
		t.Errorf("expected no errors for valid workflow, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_ValidWorkflowAsAny(t *testing.T) {
	var wf any = &validWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	if len(errors) != 0 {
		t.Errorf("expected no errors for valid workflow passed as any, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_EmptyName(t *testing.T) {
	wf := &emptyNameWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Name()") && contains(err, "non-empty") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about empty Name(), got: %v", errors)
	}
}

func TestValidateAnyWorkflow_ZeroSchemaVersion(t *testing.T) {
	wf := &zeroSchemaVersionWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "SchemaVersion()") && contains(err, "positive") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about zero SchemaVersion(), got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MissingDecide(t *testing.T) {
	wf := &missingDecideWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Decide()") && (contains(err, "2 parameters") || contains(err, "2 values")) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Decide() signature, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongDecideParams(t *testing.T) {
	wf := &wrongDecideParamsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Decide()") && contains(err, "model.State") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Decide() parameter types, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongDecideReturns(t *testing.T) {
	wf := &wrongDecideReturnsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Decide()") && (contains(err, "slice") || contains(err, "Rejection")) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Decide() return types, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MissingEvolve(t *testing.T) {
	wf := &missingEvolveWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Evolve()") && contains(err, "2 parameters") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Evolve() signature, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongEvolveParams(t *testing.T) {
	wf := &wrongEvolveParamsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Evolve()") && contains(err, "model.State") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Evolve() parameter types, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongEvolveReturn(t *testing.T) {
	wf := &wrongEvolveReturnWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Evolve()") && contains(err, "model.State") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Evolve() return type, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MissingEventToCmd(t *testing.T) {
	wf := &missingEventToCmdWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "EventToCmd()") && contains(err, "1 parameter") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about EventToCmd() signature, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongEventToCmdParams(t *testing.T) {
	wf := &wrongEventToCmdParamsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "EventToCmd()") && contains(err, "model.Event") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about EventToCmd() parameter type, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongEventToCmdReturn(t *testing.T) {
	wf := &wrongEventToCmdReturnWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "EventToCmd()") && contains(err, "model.Command") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about EventToCmd() return type, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MissingIsFinalEvent(t *testing.T) {
	wf := &missingIsFinalEventWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "IsFinalEvent()") && contains(err, "1 parameter") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about IsFinalEvent() signature, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongIsFinalEventParams(t *testing.T) {
	wf := &wrongIsFinalEventParamsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "IsFinalEvent()") && contains(err, "model.Event") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about IsFinalEvent() parameter type, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongIsFinalEventReturn(t *testing.T) {
	wf := &wrongIsFinalEventReturnWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "IsFinalEvent()") && contains(err, "bool") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about IsFinalEvent() return type, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MissingUpcast(t *testing.T) {
	wf := &missingUpcastWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Upcast()") && contains(err, "not found") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about missing Upcast(), got: %v", errors)
	}
}

func TestValidateAnyWorkflow_WrongUpcastSignature(t *testing.T) {
	wf := &wrongUpcastSignatureWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	found := false
	for _, err := range errors {
		if contains(err, "Upcast()") && contains(err, "3 parameters") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error about Upcast() signature, got: %v", errors)
	}
}

func TestValidateAnyWorkflow_MultipleErrors(t *testing.T) {
	wf := &multipleErrorsWorkflow{}
	errors := ValidateAnyWorkflow(wf)

	// Should have at least: Name, SchemaVersion, Upcast, Decide, Evolve, EventToCmd, IsFinalEvent errors
	if len(errors) < 7 {
		t.Errorf("expected at least 7 errors for multiple errors workflow, got %d: %v", len(errors), errors)
	}

	// Check for specific error categories
	categories := map[string]bool{
		"Name()":         false,
		"SchemaVersion":  false,
		"Upcast()":       false,
		"Decide()":       false,
		"Evolve()":       false,
		"EventToCmd()":   false,
		"IsFinalEvent()": false,
	}

	for _, err := range errors {
		for cat := range categories {
			if contains(err, cat) {
				categories[cat] = true
			}
		}
	}

	for cat, found := range categories {
		if !found {
			t.Errorf("expected error about %s in multiple errors workflow", cat)
		}
	}
}

// =============================================================================
// Tests for DiscoverAndValidate (placeholder)
// =============================================================================

func TestDiscoverAndValidate_Placeholder(t *testing.T) {
	result := DiscoverAndValidate("/some/path")

	if result == nil {
		t.Error("expected non-nil map from DiscoverAndValidate")
	}

	if len(result) != 0 {
		t.Errorf("expected empty map from DiscoverAndValidate placeholder, got: %v", result)
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Package validation provides workflow class validation utilities.
//
// This package is designed to validate that workflow implementations conform
// to the model.Workflow interface requirements at runtime. The primary use
// case is catching configuration errors early, particularly when workflows
// are registered dynamically or loaded from plugins.
//
// Two validation modes are provided:
//   - ValidateWorkflow: For types that already implement model.Workflow
//   - ValidateAnyWorkflow: For any type, using pure reflection (for plugins, dynamic loading)
//
// Future versions may extend DiscoverAndValidate to use go/packages or AST
// parsing for more thorough static analysis.
package validation

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// ValidateAnyWorkflow validates any type as a workflow implementation using reflection.
// This function is designed for dynamic loading scenarios where the type may not
// statically implement model.Workflow (e.g., plugins, reflection-based discovery).
//
// Checks performed:
//   - Name() returns a non-empty string
//   - SchemaVersion() returns a positive integer
//   - Upcast method exists and is callable
//   - Decide exists with proper signature: func(State, Command) ([]Event, *Rejection)
//   - Evolve exists with proper signature: func(State, Event) State
//   - EventToCmd exists with proper signature: func(Event) Command
//   - IsFinalEvent exists with proper signature: func(Event) bool
//
// Returns a slice of error strings. Empty slice indicates valid workflow.
func ValidateAnyWorkflow(workflow any) []string {
	var errors []string

	v := reflect.ValueOf(workflow)
	t := v.Type()

	// Note: We do NOT dereference pointer types here because methods with
	// pointer receivers are associated with the pointer type, not the element type.

	// Check Name()
	nameErrors := checkNameMethod(t, v)
	errors = append(errors, nameErrors...)

	// Check SchemaVersion()
	schemaErrors := checkSchemaVersionMethod(t, v)
	errors = append(errors, schemaErrors...)

	// Check Upcast exists
	upcastErrors := checkUpcastMethod(t)
	errors = append(errors, upcastErrors...)

	// Check Decide signature
	decideErrors := checkDecideMethod(t)
	errors = append(errors, decideErrors...)

	// Check Evolve signature
	evolveErrors := checkEvolveMethod(t)
	errors = append(errors, evolveErrors...)

	// Check EventToCmd signature
	eventToCmdErrors := checkEventToCmdMethod(t)
	errors = append(errors, eventToCmdErrors...)

	// Check IsFinalEvent signature
	isFinalEventErrors := checkIsFinalEventMethod(t)
	errors = append(errors, isFinalEventErrors...)

	return errors
}

// checkNameMethod validates the Name() method exists and returns a non-empty string.
func checkNameMethod(t reflect.Type, v reflect.Value) []string {
	var errors []string

	method, ok := t.MethodByName("Name")
	if !ok {
		errors = append(errors, "Name() method not found")
		return errors
	}

	// Check signature: no params (excluding receiver), one string return
	if method.Type.NumIn() != 1 { // 1 for receiver only
		errors = append(errors, fmt.Sprintf("Name() must have 0 parameters, got %d", method.Type.NumIn()-1))
	}
	if method.Type.NumOut() != 1 || method.Type.Out(0).Kind() != reflect.String {
		errors = append(errors, "Name() must return string")
	}

	// If signature is valid, call it and check the value
	if len(errors) == 0 {
		result := v.MethodByName("Name").Call(nil)
		if len(result) == 1 {
			name := result[0].String()
			if name == "" {
				errors = append(errors, "Name() must return a non-empty string")
			} else if strings.TrimSpace(name) != name {
				errors = append(errors, fmt.Sprintf("Name() must not have leading/trailing whitespace: %q", name))
			}
		}
	}

	return errors
}

// checkSchemaVersionMethod validates the SchemaVersion() method exists and returns a positive int.
func checkSchemaVersionMethod(t reflect.Type, v reflect.Value) []string {
	var errors []string

	method, ok := t.MethodByName("SchemaVersion")
	if !ok {
		errors = append(errors, "SchemaVersion() method not found")
		return errors
	}

	// Check signature: no params (excluding receiver), one int return
	if method.Type.NumIn() != 1 { // 1 for receiver only
		errors = append(errors, fmt.Sprintf("SchemaVersion() must have 0 parameters, got %d", method.Type.NumIn()-1))
	}
	if method.Type.NumOut() != 1 || method.Type.Out(0).Kind() != reflect.Int {
		errors = append(errors, "SchemaVersion() must return int")
	}

	// If signature is valid, call it and check the value
	if len(errors) == 0 {
		result := v.MethodByName("SchemaVersion").Call(nil)
		if len(result) == 1 {
			schemaVersion := result[0].Int()
			if schemaVersion <= 0 {
				errors = append(errors, fmt.Sprintf("SchemaVersion() must return a positive integer, got %d", schemaVersion))
			}
		}
	}

	return errors
}

// checkUpcastMethod validates the Upcast method exists with the correct signature.
// Expected: func(eventType string, schemaVersion int, rawData map[string]any) map[string]any
func checkUpcastMethod(t reflect.Type) []string {
	var errors []string

	method, ok := t.MethodByName("Upcast")
	if !ok {
		errors = append(errors, "Upcast() method not found")
		return errors
	}

	fnType := method.Type

	if fnType.NumIn() != 4 { // 3 params + 1 receiver
		errors = append(errors, fmt.Sprintf("Upcast() must have 3 parameters (string, int, map[string]any), got %d", fnType.NumIn()-1))
	} else {
		// Check first param is string (index 1, skipping receiver at 0)
		if fnType.In(1).Kind() != reflect.String {
			errors = append(errors, fmt.Sprintf("Upcast() first parameter must be string, got %v", fnType.In(1).Kind()))
		}
		// Check second param is int
		if fnType.In(2).Kind() != reflect.Int {
			errors = append(errors, fmt.Sprintf("Upcast() second parameter must be int, got %v", fnType.In(2).Kind()))
		}
		// Check third param is map[string]any
		if !isMapStringAny(fnType.In(3)) {
			errors = append(errors, fmt.Sprintf("Upcast() third parameter must be map[string]any, got %v", fnType.In(3)))
		}
	}

	if fnType.NumOut() != 1 {
		errors = append(errors, fmt.Sprintf("Upcast() must return 1 value (map[string]any), got %d", fnType.NumOut()))
	} else if !isMapStringAny(fnType.Out(0)) {
		errors = append(errors, fmt.Sprintf("Upcast() must return map[string]any, got %v", fnType.Out(0)))
	}

	return errors
}

// isMapStringAny checks if a type is map[string]any.
func isMapStringAny(t reflect.Type) bool {
	if t.Kind() != reflect.Map {
		return false
	}
	if t.Key().Kind() != reflect.String {
		return false
	}
	// map[string]any has value type interface{}
	return t.Elem().Kind() == reflect.Interface
}

// checkDecideMethod validates the Decide method exists with the correct signature.
// Expected: func(state model.State, cmd model.Command) ([]model.Event, *model.Rejection)
func checkDecideMethod(t reflect.Type) []string {
	var errors []string

	method, ok := t.MethodByName("Decide")
	if !ok {
		errors = append(errors, "Decide() method not found")
		return errors
	}

	errors = append(errors, validateDecideSignature(method.Type)...)
	return errors
}

// validateDecideSignature checks that the Decide method has the correct signature.
func validateDecideSignature(fnType reflect.Type) []string {
	var errors []string

	if fnType.NumIn() != 3 { // 2 params + 1 receiver
		errors = append(errors, fmt.Sprintf("Decide() must have 2 parameters (State, Command), got %d", fnType.NumIn()-1))
	} else {
		// Check first param is model.State (index 1, skipping receiver at 0)
		stateType := fnType.In(1)
		stateInterface := reflect.TypeOf((*model.State)(nil)).Elem()
		if !stateType.Implements(stateInterface) && stateType != stateInterface {
			errors = append(errors, fmt.Sprintf("Decide() first parameter must implement model.State, got %v", stateType))
		}

		// Check second param is model.Command
		cmdType := fnType.In(2)
		cmdInterface := reflect.TypeOf((*model.Command)(nil)).Elem()
		if !cmdType.Implements(cmdInterface) && cmdType != cmdInterface {
			errors = append(errors, fmt.Sprintf("Decide() second parameter must implement model.Command, got %v", cmdType))
		}
	}

	if fnType.NumOut() != 2 {
		errors = append(errors, fmt.Sprintf("Decide() must return 2 values ([]Event, *Rejection), got %d", fnType.NumOut()))
	} else {
		// Check first return is slice of model.Event
		firstReturn := fnType.Out(0)
		if firstReturn.Kind() != reflect.Slice {
			errors = append(errors, fmt.Sprintf("Decide() first return must be a slice, got %v", firstReturn.Kind()))
		} else {
			eventInterface := reflect.TypeOf((*model.Event)(nil)).Elem()
			elemType := firstReturn.Elem()
			if !elemType.Implements(eventInterface) && elemType != eventInterface {
				errors = append(errors, fmt.Sprintf("Decide() first return must be []model.Event, got []%v", elemType))
			}
		}

		// Check second return is *model.Rejection
		secondReturn := fnType.Out(1)
		rejectionType := reflect.TypeOf((*model.Rejection)(nil))
		if secondReturn != rejectionType {
			errors = append(errors, fmt.Sprintf("Decide() second return must be *model.Rejection, got %v", secondReturn))
		}
	}

	return errors
}

// checkEvolveMethod validates the Evolve method exists with the correct signature.
// Expected: func(state model.State, event model.Event) model.State
func checkEvolveMethod(t reflect.Type) []string {
	var errors []string

	method, ok := t.MethodByName("Evolve")
	if !ok {
		errors = append(errors, "Evolve() method not found")
		return errors
	}

	errors = append(errors, validateEvolveSignature(method.Type)...)
	return errors
}

// validateEvolveSignature checks that the Evolve method has the correct signature.
func validateEvolveSignature(fnType reflect.Type) []string {
	var errors []string

	if fnType.NumIn() != 3 { // 2 params + 1 receiver
		errors = append(errors, fmt.Sprintf("Evolve() must have 2 parameters (State, Event), got %d", fnType.NumIn()-1))
	} else {
		stateType := fnType.In(1) // Skip receiver at index 0
		stateInterface := reflect.TypeOf((*model.State)(nil)).Elem()
		if !stateType.Implements(stateInterface) && stateType != stateInterface {
			errors = append(errors, fmt.Sprintf("Evolve() first parameter must implement model.State, got %v", stateType))
		}

		eventType := fnType.In(2)
		eventInterface := reflect.TypeOf((*model.Event)(nil)).Elem()
		if !eventType.Implements(eventInterface) && eventType != eventInterface {
			errors = append(errors, fmt.Sprintf("Evolve() second parameter must implement model.Event, got %v", eventType))
		}
	}

	if fnType.NumOut() != 1 {
		errors = append(errors, fmt.Sprintf("Evolve() must return 1 value (State), got %d", fnType.NumOut()))
	} else {
		stateInterface := reflect.TypeOf((*model.State)(nil)).Elem()
		returnType := fnType.Out(0)
		if !returnType.Implements(stateInterface) && returnType != stateInterface {
			errors = append(errors, fmt.Sprintf("Evolve() must return model.State, got %v", returnType))
		}
	}

	return errors
}

// checkEventToCmdMethod validates the EventToCmd method exists with the correct signature.
// Expected: func(e model.Event) model.Command
func checkEventToCmdMethod(t reflect.Type) []string {
	var errors []string

	method, ok := t.MethodByName("EventToCmd")
	if !ok {
		errors = append(errors, "EventToCmd() method not found")
		return errors
	}

	errors = append(errors, validateEventToCmdSignature(method.Type)...)
	return errors
}

// validateEventToCmdSignature checks that the EventToCmd method has the correct signature.
func validateEventToCmdSignature(fnType reflect.Type) []string {
	var errors []string

	if fnType.NumIn() != 2 { // 1 param + 1 receiver
		errors = append(errors, fmt.Sprintf("EventToCmd() must have 1 parameter (Event), got %d", fnType.NumIn()-1))
	} else {
		eventType := fnType.In(1) // Skip receiver at index 0
		eventInterface := reflect.TypeOf((*model.Event)(nil)).Elem()
		if !eventType.Implements(eventInterface) && eventType != eventInterface {
			errors = append(errors, fmt.Sprintf("EventToCmd() parameter must implement model.Event, got %v", eventType))
		}
	}

	if fnType.NumOut() != 1 {
		errors = append(errors, fmt.Sprintf("EventToCmd() must return 1 value (Command), got %d", fnType.NumOut()))
	} else {
		cmdInterface := reflect.TypeOf((*model.Command)(nil)).Elem()
		returnType := fnType.Out(0)
		if returnType.Kind() != reflect.Interface || !returnType.Implements(cmdInterface) {
			errors = append(errors, fmt.Sprintf("EventToCmd() must return model.Command (or nil), got %v", returnType))
		}
	}

	return errors
}

// checkIsFinalEventMethod validates the IsFinalEvent method exists with the correct signature.
// Expected: func(e model.Event) bool
func checkIsFinalEventMethod(t reflect.Type) []string {
	var errors []string

	method, ok := t.MethodByName("IsFinalEvent")
	if !ok {
		errors = append(errors, "IsFinalEvent() method not found")
		return errors
	}

	errors = append(errors, validateIsFinalEventSignature(method.Type)...)
	return errors
}

// validateIsFinalEventSignature checks that the IsFinalEvent method has the correct signature.
func validateIsFinalEventSignature(fnType reflect.Type) []string {
	var errors []string

	if fnType.NumIn() != 2 { // 1 param + 1 receiver
		errors = append(errors, fmt.Sprintf("IsFinalEvent() must have 1 parameter (Event), got %d", fnType.NumIn()-1))
	} else {
		eventType := fnType.In(1) // Skip receiver at index 0
		eventInterface := reflect.TypeOf((*model.Event)(nil)).Elem()
		if !eventType.Implements(eventInterface) && eventType != eventInterface {
			errors = append(errors, fmt.Sprintf("IsFinalEvent() parameter must implement model.Event, got %v", eventType))
		}
	}

	if fnType.NumOut() != 1 {
		errors = append(errors, fmt.Sprintf("IsFinalEvent() must return 1 value (bool), got %d", fnType.NumOut()))
	} else if fnType.Out(0).Kind() != reflect.Bool {
		errors = append(errors, fmt.Sprintf("IsFinalEvent() must return bool, got %v", fnType.Out(0).Kind()))
	}

	return errors
}

// ValidateWorkflow validates a workflow that already implements model.Workflow.
// This is a convenience wrapper around ValidateAnyWorkflow for typed usage.
//
// Since the type already satisfies the interface at compile time, this primarily
// validates runtime values (Name non-empty, SchemaVersion positive).
//
// Example usage:
//
//	wf := &MyWorkflow{}
//	errors := validation.ValidateWorkflow(wf)
//	if len(errors) > 0 {
//	    for _, err := range errors {
//	        log.Printf("workflow validation error: %s", err)
//	    }
//	}
func ValidateWorkflow(workflow model.Workflow) []string {
	return ValidateAnyWorkflow(workflow)
}

// DiscoverAndValidate scans a Go module for Workflow implementations and validates each.
//
// This function is intended for future use with reflection or AST-based discovery.
// For now, it returns an empty map as true module scanning requires additional
// infrastructure such as go/packages, go/ast, or plugin support.
//
// Parameters:
//   - modulePath: filesystem path to the Go module to scan
//
// Returns:
//   - map[class_name][]errors: mapping of workflow class names to their validation errors.
//     Empty error slice indicates a valid workflow. Only workflows with errors are included.
//
// Example future implementation:
//
//	cfg := &packages.Config{Mode: packages.NeedTypes | packages.NeedTypesInfo, Dir: modulePath}
//	pkgs, _ := packages.Load(cfg, "./...")
//	result := make(map[string][]string)
//	for _, pkg := range pkgs {
//	    scope := pkg.Types.Scope()
//	    for _, name := range scope.Names() {
//	        obj := scope.Lookup(name)
//	        if implementsWorkflow(obj.Type()) {
//	            instance := reflect.New(obj.Type()).Elem().Interface()
//	            errs := ValidateAnyWorkflow(instance)
//	            if len(errs) > 0 {
//	                result[name] = errs
//	            }
//	        }
//	    }
//	}
//	return result
func DiscoverAndValidate(modulePath string) map[string][]string {
	// TODO: Implement module scanning using go/packages or go/ast
	return make(map[string][]string)
}

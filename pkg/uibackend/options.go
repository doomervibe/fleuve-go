package uibackend

// Option configures FleuveUIBackend construction.
type Option func(*FleuveUIBackend)

// WithoutBundledUI disables the embedded default UI (API-only mode, e.g. for tests).
func WithoutBundledUI() Option {
	return func(b *FleuveUIBackend) {
		b.disableBundledUI = true
		b.frontendFS = nil
	}
}

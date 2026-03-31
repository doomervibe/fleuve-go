// Package uiembed ships the Fleuve admin UI (vendored frontend_dist from the Python Fleuve UI build) embedded in the binary.
package uiembed

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var root embed.FS

// Dist is the contents of dist/ (index.html at the root of this FS).
var Dist = mustSub(root, "dist")

func mustSub(fsys embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic("uiembed: " + err.Error())
	}
	return sub
}

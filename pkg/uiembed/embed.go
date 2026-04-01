package uiembed

import (
	"bytes"
	"embed"
	"io/fs"
)

//go:embed dist
var dist embed.FS

// DistFS is the embedded dist directory (index.html, assets/, …).
func DistFS() fs.FS {
	f, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return f
}

// IndexHTML returns index.html with {{project_title}} replaced like Python
// (FleuveUIBackend._serve_index_html): placeholder becomes uiTitle; the template
// keeps the literal " UI" suffix in the document.
func IndexHTML(uiTitle string) []byte {
	raw, err := dist.ReadFile("dist/index.html")
	if err != nil {
		return []byte("<!doctype html><html><body>missing embedded UI dist</body></html>")
	}
	if uiTitle == "" {
		uiTitle = "Fleuve"
	}
	return bytes.ReplaceAll(raw, []byte("{{project_title}}"), []byte(uiTitle))
}

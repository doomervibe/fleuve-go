package uiembed

import (
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// ResolveUITitle matches Python FleuveUIBackend: FLEUVE_UI_TITLE or cwd basename
// with underscores replaced by spaces and Title-cased.
func ResolveUITitle() string {
	if t := os.Getenv("FLEUVE_UI_TITLE"); t != "" {
		return t
	}
	wd, err := os.Getwd()
	if err != nil {
		return "Fleuve"
	}
	base := path.Base(wd)
	s := strings.ReplaceAll(base, "_", " ")
	return cases.Title(language.English).String(s)
}

// NewHandler serves the embedded React app: GET /assets/*, static files from
// dist root when present, otherwise SPA fallback to index.html (mirrors Python
// catch-all). uiTitle is passed to IndexHTML; use ResolveUITitle() to match Python.
func NewHandler(uiTitle string) http.Handler {
	root := DistFS()
	assetsFS, err := fs.Sub(root, "assets")
	if err != nil {
		assetsFS = root // empty; /assets/ may 404
	}
	assetsHandler := http.StripPrefix("/assets", http.FileServer(http.FS(assetsFS)))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		upath := path.Clean("/" + r.URL.Path)
		if upath == "/index.html" {
			upath = "/"
		}
		if strings.HasPrefix(upath, "/assets/") || upath == "/assets" {
			assetsHandler.ServeHTTP(w, r)
			return
		}
		rel := strings.TrimPrefix(upath, "/")
		if rel != "" && rel != "." {
			f, err := root.Open(rel)
			if err == nil {
				st, err := f.Stat()
				if err == nil && !st.IsDir() {
					defer f.Close()
					serveFile(w, r, rel, f, st)
					return
				}
				f.Close()
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(IndexHTML(uiTitle))
	})
}

func serveFile(w http.ResponseWriter, r *http.Request, name string, f fs.File, st fs.FileInfo) {
	switch path.Ext(name) {
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".json":
		w.Header().Set("Content-Type", "application/json")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".ico":
		w.Header().Set("Content-Type", "image/x-icon")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
		return
	}
	_, _ = io.Copy(w, f)
}

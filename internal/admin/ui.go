package admin

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui
var uiFiles embed.FS

// uiCSP only allows same-origin scripts, styles and API calls: the console
// has no inline scripts and loads nothing from a CDN.
const uiCSP = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// uiRoutes serves the web console at /ui/. The static files are public; all
// data comes from /api/*, which keeps its authentication.
func (s *Server) uiRoutes() {
	sub, err := fs.Sub(uiFiles, "ui")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	files := http.StripPrefix("/ui/", http.FileServerFS(sub))
	s.mux.Handle("GET /ui/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", uiCSP)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		// Embedded files carry no modification time, so make browsers
		// revalidate instead of caching a stale console across upgrades.
		h.Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	}))
	s.mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))
}

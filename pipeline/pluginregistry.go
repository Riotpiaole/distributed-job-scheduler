package pipeline

import (
	"fmt"
	"path/filepath"
	"sync"
)

// PluginRegistry lazily loads .so plugins from a directory on first access.
// Loaded plugins are cached — Go's plugin package does not support unloading;
// replacing a plugin requires a node restart.
type PluginRegistry struct {
	mu        sync.RWMutex
	pluginDir string
	plugins   map[string]*PluginFuncs
}

func NewPluginRegistry(dir string) *PluginRegistry {
	return &PluginRegistry{
		pluginDir: dir,
		plugins:   make(map[string]*PluginFuncs),
	}
}

// Get returns the cached plugin for name, or loads it from pluginDir/<name>.so on
// first access. name is the filename stem (e.g. "wc" for wc.so).
func (r *PluginRegistry) Get(name string) (*PluginFuncs, error) {
	r.mu.RLock()
	if pf, ok := r.plugins[name]; ok {
		r.mu.RUnlock()
		return pf, nil
	}
	r.mu.RUnlock()

	path := filepath.Join(r.pluginDir, name+".so")
	pf, err := LoadPlugin(path)
	if err != nil {
		return nil, fmt.Errorf("load plugin %q from %s: %w", name, path, err)
	}

	r.mu.Lock()
	r.plugins[name] = pf
	r.mu.Unlock()
	return pf, nil
}

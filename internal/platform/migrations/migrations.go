// Package migrations combines the shared platform migrations with a service's
// own migrations into a single filesystem that golang-migrate can consume.
//
// Platform migrations occupy version range 1; service migrations start at
// version 2. Keeping the version ranges disjoint avoids collisions when the two
// layers are merged.
package migrations

import (
	"embed"
	"io/fs"
	"sort"
)

//go:embed common
var commonFS embed.FS

// Platform returns the embedded platform migrations.
func Platform() fs.FS {
	sub, err := fs.Sub(commonFS, "common")
	if err != nil {
		panic(err)
	}
	return sub
}

// Union merges the platform migrations with serviceMigrations. The supplied
// filesystem must expose the service migration files at its root; use fs.Sub to
// strip a directory prefix.
func Union(serviceMigrations fs.FS) fs.FS {
	return unionFS{layers: []fs.FS{Platform(), serviceMigrations}}
}

// ServiceUnion strips dir from embedded and merges the result with the platform
// migrations. It is the standard way a service builds its migration filesystem.
func ServiceUnion(embedded fs.FS, dir string) (fs.FS, error) {
	sub, err := fs.Sub(embedded, dir)
	if err != nil {
		return nil, err
	}
	return Union(sub), nil
}

type unionFS struct {
	layers []fs.FS
}

func (u unionFS) Open(name string) (fs.File, error) {
	for _, layer := range u.layers {
		if f, err := layer.Open(name); err == nil {
			return f, nil
		}
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

func (u unionFS) ReadFile(name string) ([]byte, error) {
	for _, layer := range u.layers {
		if data, err := fs.ReadFile(layer, name); err == nil {
			return data, nil
		}
	}
	return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrNotExist}
}

func (u unionFS) ReadDir(name string) ([]fs.DirEntry, error) {
	seen := make(map[string]struct{})
	var merged []fs.DirEntry
	for _, layer := range u.layers {
		entries, err := fs.ReadDir(layer, name)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if _, ok := seen[e.Name()]; ok {
				continue
			}
			seen[e.Name()] = struct{}{}
			merged = append(merged, e)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Name() < merged[j].Name() })
	return merged, nil
}

var (
	_ fs.FS         = unionFS{}
	_ fs.ReadDirFS  = unionFS{}
	_ fs.ReadFileFS = unionFS{}
)

package storage

import (
	"fmt"
	"strings"
)

type Storage interface {
	Get(key string) any
	Set(key string, val any)
	Delete(key string)
	View() string
}

type VapourDB struct {
	Store map[string]any
}

func (v *VapourDB) Get(key string) any {
	val, ok := v.Store[key]
	if !ok {
		// Should return some usable data but for now returning string to write to conn
		return fmt.Sprintf("Key %s does not exist", key)
	}
	return val
}

func (v *VapourDB) Set(key string, val any) {
	v.Store[key] = val
}

func (v *VapourDB) Delete(key string) {
	delete(v.Store, key)
}

func (v *VapourDB) View() string {
	var output strings.Builder
	output.WriteString("[key]: [value]\n")
	for key, val := range v.Store {
		fmt.Fprintf(&output, "%v: %v (%T)\n", key, val, val)
	}
	return output.String()
}

func CreateDb() Storage {
	return &VapourDB{
		Store: make(map[string]any),
	}
}

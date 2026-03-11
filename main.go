package main

import (
	"os"
	"sync"
)

type Storage interface {
	Get(key string) string
	Set(key string, val string)
	Delete(key string)
}

type VaporDB struct {
	store map[string]string
	mu    sync.RWMutex
}

func (v *VaporDB) Get(key string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.store[key]
}

func (v *VaporDB) Set(key string, val string) {
	v.mu.Lock()
	v.store[key] = val
	v.mu.Unlock()
}

func (v *VaporDB) Delete(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.store, key)
}

func createDb() *VaporDB {
	return &VaporDB{
		store: make(map[string]string),
	}
}

func runCommand(s Storage, rootCommand string, key string, val string) string {
	switch rootCommand {
	case "GET":
		return s.Get(key)
	case "DELETE":
		s.Delete(key)
		return key
	case "SET":
		s.Set(key, val)
		return key
	default:
		panic("Unknown command")
	}
}

func parseCommand(cmd []string) (rootCommand string, key string, val string) {
	_rootCommand := cmd[0]
	_key := cmd[1]
	var _val string
	if len(cmd) > 2 {
		_val = cmd[2]
	}
	return _rootCommand, _key, _val
}

func main() {
	vapourDB := createDb()
	args := os.Args
	rootCommand, key, val := parseCommand(args[1:])
	runCommand(vapourDB, rootCommand, key, val)
}

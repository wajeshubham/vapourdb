package server

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/wajeshubham/vapourdb/protocol"
	"github.com/wajeshubham/vapourdb/storage"
)

var AOF_FILENAME string = "vapour.aof"

type AOF struct {
	File   *os.File
	FileMu sync.Mutex
}

func (db *DbServer) WriteAOF(cmd protocol.Command) error {
	db.AofFile.FileMu.Lock()
	defer db.AofFile.FileMu.Unlock()
	stringCommand := fmt.Sprintf("%s %s %s\n", cmd.Root, cmd.Key, cmd.RawVal)
	_, err := db.AofFile.File.WriteString(stringCommand)
	return err
}

func (db *DbServer) LoadAOF(s storage.Storage) error {
	db.AofFile.FileMu.Lock()
	defer db.AofFile.FileMu.Unlock()
	db.AofFile.File.Seek(0, io.SeekStart) // Avoid ready AOF as the File opened with O_APPEND
	scanner := bufio.NewScanner(db.AofFile.File)
	for scanner.Scan() {
		cmd := scanner.Text()
		command, err := protocol.ParseCommand(cmd)
		if err != nil {
			continue
		}
		db.ApplyCommand(s, command)

	}
	fmt.Println("Loaded AOF")
	return scanner.Err()
}

func (db *DbServer) AOFWriter() {
	for cmd := range db.AofCh {
		db.WriteAOF(cmd)
	}
}

func (db *DbServer) FsyncLoop() {
	ticker := time.NewTicker(time.Second * 2)

	for range ticker.C {
		db.AofFile.FileMu.Lock()
		db.AofFile.File.Sync()
		db.AofFile.FileMu.Unlock()
	}
}

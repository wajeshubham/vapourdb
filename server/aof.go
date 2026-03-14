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

// Scheduled fsync for the AOF file so data actually reaches disk
// the writes normally go to the OS page cache first, so without this
// a crash could lose recent commands. This loop flushes the data
// every few seconds. This is a simplified durability model though, prod systems like redis have some safeguards to improve it
func (db *DbServer) FsyncLoop() {
	ticker := time.NewTicker(time.Second * 2)

	for range ticker.C {
		db.AofFile.FileMu.Lock()
		db.AofFile.File.Sync()
		db.AofFile.FileMu.Unlock()
	}
}

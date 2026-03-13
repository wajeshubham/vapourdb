package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/wajeshubham/vapourdb/protocol"
	"github.com/wajeshubham/vapourdb/storage"
)

type RequestMessage struct {
	cs      *ConnState
	payload string
}

type DbServer struct {
	ListeningAddr string
	Ln            net.Listener
	AofFile       *AOF
	QuitCh        chan struct{}
	MsgCh         chan RequestMessage
	AofCh         chan protocol.Command
}

type ConnState struct {
	Conn    net.Conn
	WriteCh chan string
}

func (db *DbServer) Start() error {
	ln, err := net.Listen("tcp", db.ListeningAddr)
	if err != nil {
		return err
	}
	fmt.Println("Started Server")
	db.Ln = ln
	go db.AcceptLoop()
	<-db.QuitCh
	ln.Close()
	db.AofFile.FileMu.Lock()
	db.AofFile.File.Close()
	db.AofFile.FileMu.Unlock()
	fmt.Println("Closing the server")
	return nil
}

func (db *DbServer) AcceptLoop() {
	for {
		conn, err := db.Ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Started accepting connection: ", conn.RemoteAddr())
		cs := &ConnState{
			Conn:    conn,
			WriteCh: make(chan string, 32),
		}
		go db.WriteToConn(cs)
		go db.ReadConnection(cs)
	}
}

func (db *DbServer) ReadConnection(cs *ConnState) {
	defer func() {
		cs.Conn.Close()
		close(cs.WriteCh)
	}()
	reader := bufio.NewReader(cs.Conn)
	for {
		payloadLine, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				cs.WriteCh <- fmt.Sprintf("ERROR: %v", err)
			}
			return
		}
		req := RequestMessage{cs: cs, payload: payloadLine}
		db.MsgCh <- req // Forward the request msg to command handler through channel
	}
}

func (db *DbServer) HandleCommand(store storage.Storage) {
	for msg := range db.MsgCh {
		command, err := protocol.ParseCommand(msg.payload)
		if err != nil {
			msg.cs.WriteCh <- fmt.Sprintf("ERROR: %v", err)
			continue
		}
		db.ExecuteCommand(store, msg.cs, command)
	}
}

func (db *DbServer) WriteToConn(cs *ConnState) {
	for msg := range cs.WriteCh {
		fmt.Fprintf(cs.Conn, "%v\n", msg)
	}
}

func (db *DbServer) AOFWriter() {
	for cmd := range db.AofCh {
		db.WriteAOF(cmd)
	}
}

func (db *DbServer) ApplyCommand(s storage.Storage, cmd protocol.Command) {
	switch cmd.Root {
	case "SET":
		s.Set(cmd.Key, cmd.Val)
	case "DELETE":
		s.Delete(cmd.Key)
	}
}

func (db *DbServer) ExecuteCommand(s storage.Storage, cs *ConnState, cmd protocol.Command) {
	switch cmd.Root {
	case "GET":
		val := s.Get(cmd.Key)
		cs.WriteCh <- fmt.Sprint(val)
	case "DELETE":
		db.AofCh <- cmd
		s.Delete(cmd.Key)
		cs.WriteCh <- cmd.Key
	case "SET":
		db.AofCh <- cmd // This might result in out of order commands in AOF only when doing concurrent cmd handling (which we are not)
		s.Set(cmd.Key, cmd.Val)
		cs.WriteCh <- cmd.Key
	case "VIEW":
		op := s.View()
		cs.WriteCh <- op
	default:
		cs.WriteCh <- "UNKNOWN COMMAND"
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

func NewServer(L string) *DbServer {
	aofFile, err := os.OpenFile(AOF_FILENAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return &DbServer{
		ListeningAddr: L,
		QuitCh:        make(chan struct{}, 1),
		MsgCh:         make(chan RequestMessage, 1024),
		AofFile:       &AOF{File: aofFile, FileMu: sync.Mutex{}},
		AofCh:         make(chan protocol.Command, 1024),
	}
}

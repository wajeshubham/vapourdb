package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var SUPPORTED_COMMANDS []string = []string{"GET", "SET", "DELETE", "VIEW"}
var MAX_GOROUTINES int = 4

type Command struct {
	Root string
	Key  string
	Val  any
}

type Storage interface {
	Get(key string) any
	Set(key string, val any)
	Delete(key string)
	View() string
}

type RequestMessage struct {
	cs      *ConnState
	payload string
}

type DbServer struct {
	listeningAddr string
	ln            net.Listener
	quitCh        chan struct{}
	msgCh         chan RequestMessage
}

type ConnState struct {
	conn    net.Conn
	writeCh chan string
}

func (db *DbServer) Start() error {
	ln, err := net.Listen("tcp", db.listeningAddr)
	if err != nil {
		return err
	}
	fmt.Println("Started Server")
	defer ln.Close()
	db.ln = ln
	go db.AcceptLoop()
	<-db.quitCh
	fmt.Println("Closing the server")
	return nil
}

func (db *DbServer) AcceptLoop() {
	for {
		conn, err := db.ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Started accepting connection: ", conn.RemoteAddr())
		cs := &ConnState{
			conn:    conn,
			writeCh: make(chan string, 32),
		}
		go WriteToConn(cs)
		go db.ReadConnection(cs)
	}
}

func (db *DbServer) ReadConnection(cs *ConnState) {
	defer func() {
		cs.conn.Close()
		close(cs.writeCh)
	}()
	reader := bufio.NewReader(cs.conn)
	for {
		payloadLine, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				cs.writeCh <- fmt.Sprintf("ERROR: %v", err)
			}
			return
		}
		req := RequestMessage{cs: cs, payload: payloadLine}
		db.msgCh <- req
	}
}

func (db *DbServer) HandleCommand(store Storage) {
	for msg := range db.msgCh {
		command, err := ParseCommand(msg.payload)
		if err != nil {
			msg.cs.writeCh <- fmt.Sprintf("ERROR: %v", err)
			continue
		}
		ExecuteCommand(store, msg.cs, command.Root, command.Key, command.Val)
	}
}

type VapourDB struct {
	store map[string]any
	mu    sync.RWMutex
}

func (v *VapourDB) Get(key string) any {
	v.mu.RLock()
	defer v.mu.RUnlock()
	val, ok := v.store[key]
	if !ok {
		return fmt.Sprintf("Key %s does not exist", key)
	}
	return val
}

func (v *VapourDB) Set(key string, val any) {
	v.mu.Lock()
	v.store[key] = val
	v.mu.Unlock()
}

func (v *VapourDB) Delete(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.store, key)
}

func (v *VapourDB) View() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var output strings.Builder
	output.WriteString("[key]: [value]\n")
	for key, val := range v.store {
		fmt.Fprintf(&output, "%v: %v (%T)\n", key, val, val)
	}
	return output.String()
}

func NewServer(listeningAddr string) *DbServer {
	return &DbServer{
		listeningAddr: listeningAddr,
		quitCh:        make(chan struct{}, 1),
		msgCh:         make(chan RequestMessage, MAX_GOROUTINES*10),
	}
}

func CreateDb() Storage {
	return &VapourDB{
		store: make(map[string]any),
	}
}

func ParseCommand(cmd string) (Command, error) {
	cmd = strings.TrimSpace(cmd)
	command := strings.Fields(cmd)
	var err error = nil
	if len(command) <= 0 {
		return Command{}, fmt.Errorf("Invalid command %s\n", cmd)
	}
	rootCommand := command[0]
	var key string
	var val any
	if !slices.Contains(SUPPORTED_COMMANDS, rootCommand) {
		return Command{}, fmt.Errorf("Unsupported command %s\n", rootCommand)
	}
	if len(command) > 1 {
		key = command[1]
	}
	if len(command) > 2 {
		var _val string
		_val = strings.Join(command[2:], " ")
		_val = strings.TrimSpace(_val)
		if strings.HasPrefix(_val, `"`) && strings.HasSuffix(_val, `"`) {
			val, err = strconv.Unquote(_val)
		} else if strings.ToLower(_val) == "true" || strings.ToLower(_val) == "false" {
			val, err = strconv.ParseBool(_val)
		} else if strings.Contains(_val, ".") {
			val, err = strconv.ParseFloat(_val, 64)
		} else {
			val, err = strconv.ParseInt(_val, 10, 64)
		}
		if err != nil {
			val = _val
		}
	}
	return Command{
		Root: rootCommand,
		Key:  key,
		Val:  val,
	}, err
}

func WriteToConn(cs *ConnState) {
	for msg := range cs.writeCh {
		fmt.Fprintf(cs.conn, "%v\n", msg)
	}
}

func ExecuteCommand(s Storage, cs *ConnState, rootCommand string, key string, val any) {
	switch rootCommand {
	case "GET":
		val := s.Get(key)
		cs.writeCh <- fmt.Sprint(val)
	case "DELETE":
		s.Delete(key)
		cs.writeCh <- key
	case "SET":
		s.Set(key, val)
		cs.writeCh <- key
	case "VIEW":
		op := s.View()
		cs.writeCh <- op
	default:
		cs.writeCh <- "UNKNOWN COMMAND"
	}
}

func main() {
	vapourDB := CreateDb()
	dbServer := NewServer(":8080")
	for i := 0; i < MAX_GOROUTINES; i++ {
		go func() {
			dbServer.HandleCommand(vapourDB)
		}()
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalCh
		dbServer.quitCh <- struct{}{}
	}()

	log.Fatal(dbServer.Start())
}

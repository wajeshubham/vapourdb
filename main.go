package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
)

var SUPPORTED_COMMANDS []string = []string{"GET", "SET", "DELETE", "VIEW"}
var MAX_GOROUTINES int = 4

type Storage interface {
	Get(key string) any
	Set(key string, val any)
	Delete(key string)
	View() string
}

type RequestMessage struct {
	conn    net.Conn
	payload string
}

type RequestError struct {
	conn net.Conn
	err  error
}

type DbServer struct {
	listeningAddr string
	ln            net.Listener
	quitCh        chan struct{}
	msgCh         chan RequestMessage
	errorCh       chan RequestError
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
	close(db.errorCh)
	close(db.msgCh)
	fmt.Println("Closing the server")
	return nil
}

func (db *DbServer) AcceptLoop() error {
	for {
		conn, err := db.ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("Started accepting connection: ", conn.RemoteAddr())
		go db.ReadConnection(conn)
	}
}

func (db *DbServer) ReadConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		payloadLine, err := reader.ReadString('\n')
		if err != nil {
			db.errorCh <- RequestError{conn: conn, err: err}
			return
		}
		req := RequestMessage{conn: conn, payload: payloadLine}
		db.msgCh <- req
	}
}

func (db *DbServer) HandleCommand(store *VapourDB) {
	for msg := range db.msgCh {
		rootCommand, key, val, err := ParseCommand(msg.payload)
		if err != nil {
			if err == io.EOF {
				return
			}
			db.errorCh <- RequestError{conn: msg.conn, err: err}
			continue
		}
		ExecuteCommand(store, msg.conn, rootCommand, key, val)
	}
}

func (db *DbServer) HandleErrors() {
	for err := range db.errorCh {
		fmt.Fprintf(err.conn, "ERROR: %v", err.err)
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
		quitCh:        make(chan struct{}, 500),
		msgCh:         make(chan RequestMessage, 500),
		errorCh:       make(chan RequestError, 500),
	}
}

func CreateDb() *VapourDB {
	return &VapourDB{
		store: make(map[string]any),
	}
}

func ParseCommand(cmd string) (rootCommand string, key string, val any, err error) {
	cmd = strings.TrimSpace(cmd)
	command := strings.Fields(cmd)
	err = nil
	if len(command) <= 0 {
		return "", "", "", fmt.Errorf("Invalid command %s\n", cmd)
	}
	rootCommand = command[0]
	if !slices.Contains(SUPPORTED_COMMANDS, rootCommand) {
		return "", "", "", fmt.Errorf("Unsupported command %s\n", rootCommand)
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
	}
	return rootCommand, key, val, err
}

func ExecuteCommand(s Storage, conn net.Conn, rootCommand string, key string, val any) {
	switch rootCommand {
	case "GET":
		val := s.Get(key)
		fmt.Fprintf(conn, "%v\n", val)
	case "DELETE":
		s.Delete(key)
		fmt.Fprintf(conn, "%v\n", key)
	case "SET":
		s.Set(key, val)
		fmt.Fprintf(conn, "%v\n", key)
	case "VIEW":
		op := s.View()
		fmt.Fprintf(conn, "%v\n", op)

	default:
		panic("Unknown command")
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
	go dbServer.HandleErrors()
	log.Fatal(dbServer.Start())
}

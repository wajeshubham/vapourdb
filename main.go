package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wajeshubham/vapourdb/server"
	"github.com/wajeshubham/vapourdb/storage"
)

func main() {
	// create db and server
	vapourDB := storage.CreateDb()
	dbServer := server.NewServer(":8080")

	// Load AOF to persist the data before restart
	if err := dbServer.LoadAOF(vapourDB); err != nil {
		log.Fatal("failed to load AOF:", err)
	}
	// Listen to AOF writer channel
	go dbServer.AOFWriter()
	// Schedule an fsync to flush data to memory
	go dbServer.FsyncLoop()

	// Handle network command
	go dbServer.HandleCommand(vapourDB)

	// Handle closure gracefully
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalCh
		dbServer.QuitCh <- struct{}{}
	}()

	// start the server
	log.Fatal(dbServer.Start())
}

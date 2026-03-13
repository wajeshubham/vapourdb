# VapourDB

VapourDB is a small in-memory key-value database written in Go.
It exposes a simple TCP server where clients can run commands like `SET`, `GET`, `DELETE`, and `VIEW`.

The goal of this project wasn't to build the next production database, but to understand how databases, networking, and persistence actually work under the hood. ALtough you can give this db a try :P

## Features

- TCP server for client connections
- In memory key-val store
- Append Only File (AOF) persistence
- Scheduled `fsync` for durability
- Single command execution at a time with single goroutine

## Example

```
SET name "vapour"
SET isMale true
SET age 20
GET name
DELETE name
GET age
VIEW
```

## Project Structure

```
protocol/   → command parsing
server/     → TCP server + AOF handling
storage/    → database implementation
main.go     → entry point
```

## Why I Built This

I built VapourDB while exploring how simple databases work internally — networking, command parsing, append-only logs, and concurrency.

And yes, this project was written **without using AI to generate the code** and completely driven by curiosity

## Disclaimer

This is an educational project, not a production database.
but it was a fun way to understand how systems like in memory dbs actually work.

## Running

```
go run main.go
```

Then connect using `telnet` or `nc`:

```
nc localhost 8080
```

package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

var SUPPORTED_COMMANDS map[string]int = map[string]int{"GET": 2, "SET": 3, "DELETE": 2, "VIEW": 1}

type Command struct {
	Root   string
	Key    string
	Val    any
	RawVal string
}

func ParseCommand(cmd string) (Command, error) {
	cmd = strings.TrimSpace(cmd)
	command := strings.Fields(cmd)
	var err error = nil
	if len(command) <= 0 {
		return Command{}, fmt.Errorf("Invalid command %s\n", cmd)
	}
	rootCommand := command[0]
	var _val string
	var key string
	var val any
	expectedLength, ok := SUPPORTED_COMMANDS[rootCommand]
	if !ok {
		return Command{}, fmt.Errorf("Unsupported command %s\n", rootCommand)
	}
	if len(command) < expectedLength {
		return Command{}, fmt.Errorf("Invalid arguments for %s\n", rootCommand)
	}
	if len(command) > 1 {
		key = command[1]
	}
	if len(command) > 2 {
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
			fmt.Println("Parsing ERROR: ", err)
			fmt.Println("Fallback to string type to handle error silently")
			err = nil
			val = _val
		}
	}
	return Command{
		Root:   rootCommand,
		Key:    key,
		Val:    val,
		RawVal: _val,
	}, err
}

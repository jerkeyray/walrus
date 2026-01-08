package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/jerkeyray/walrus/store"
	"github.com/jerkeyray/walrus/wal"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[37m"
	colorBold   = "\033[1m"
)

func printSuccess(msg string) {
	fmt.Printf("%s%s%s\n", colorGreen, msg, colorReset)
}

func printError(msg string) {
	fmt.Printf("%s%s%s\n", colorRed, msg, colorReset)
}

func printInfo(msg string) {
	fmt.Printf("%s%s%s\n", colorCyan, msg, colorReset)
}

func printWarning(msg string) {
	fmt.Printf("%s%s%s\n", colorYellow, msg, colorReset)
}

func printBanner() {
	banner := `	
 ___       __   ________  ___       ________  ___  ___  ________      
|\  \     |\  \|\   __  \|\  \     |\   __  \|\  \|\  \|\   ____\     
\ \  \    \ \  \ \  \|\  \ \  \    \ \  \|\  \ \  \\\  \ \  \___|_    
 \ \  \  __\ \  \ \   __  \ \  \    \ \   _  _\ \  \\\  \ \_____  \   
  \ \  \|\__\_\  \ \  \ \  \ \  \____\ \  \\  \\ \  \\\  \|____|\  \  
   \ \____________\ \__\ \__\ \_______\ \__\\ _\\ \_______\____\_\  \ 
    \|____________|\|__|\|__|\|_______|\|__|\|__|\|_______|\_________\
                                                          \|_________|


`
	fmt.Print(colorCyan + banner + colorReset)
	fmt.Println(colorGray + "A simple persistent key-value store with WAL" + colorReset)
	fmt.Println(colorGray + "Type 'help' for available commands" + colorReset)
	fmt.Println()
}

func printHelp() {
	help := `
` + colorBold + "Available Commands:" + colorReset + `

  ` + colorGreen + `SET` + colorReset + ` <key> <value>     Store a key-value pair
  ` + colorGreen + `GET` + colorReset + ` <key>             Retrieve value for a key
  ` + colorGreen + `DELETE` + colorReset + ` <key>          Remove a key
  ` + colorGreen + `HAS` + colorReset + ` <key>             Check if key exists
  ` + colorGreen + `KEYS` + colorReset + `                  List all keys
  ` + colorGreen + `LEN` + colorReset + `                   Show number of keys
  ` + colorGreen + `COMMIT` + colorReset + `                Flush all pending writes
  ` + colorGreen + `CLEAR` + colorReset + `                 Clear the screen
  ` + colorGreen + `HELP` + colorReset + `                  Show this help message
  ` + colorGreen + `EXIT` + colorReset + `                  Exit the CLI

` + colorBold + "Examples:" + colorReset + `
  walrus> SET name jerk
  walrus> GET name
  walrus> DELETE name
  walrus> KEYS
`
	fmt.Println(help)
}

func handleCommand(s *store.Store, parts []string) {
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "SET":
		if len(parts) < 3 {
			printError("Usage: SET <key> <value>")
			return
		}
		key := parts[1]
		value := strings.Join(parts[2:], " ")

		if err := s.Set(key, value); err != nil {
			printError(fmt.Sprintf("Error: %v", err))
			return
		}
		printSuccess(fmt.Sprintf("OK (set '%s' = '%s')", key, value))

	case "GET":
		if len(parts) < 2 {
			printError("Usage: GET <key>")
			return
		}
		key := parts[1]

		value, ok := s.Get(key)
		if !ok {
			printWarning(fmt.Sprintf("Key '%s' not found", key))
			return
		}
		printInfo(fmt.Sprintf("%s", value))

	case "DELETE", "DEL":
		if len(parts) < 2 {
			printError("Usage: DELETE <key>")
			return
		}
		key := parts[1]

		if !s.Has(key) {
			printWarning(fmt.Sprintf("Key '%s' does not exist", key))
			return
		}

		if err := s.Delete(key); err != nil {
			printError(fmt.Sprintf("Error: %v", err))
			return
		}
		printSuccess(fmt.Sprintf("OK (deleted '%s')", key))

	case "HAS", "EXISTS":
		if len(parts) < 2 {
			printError("Usage: HAS <key>")
			return
		}
		key := parts[1]

		if s.Has(key) {
			printSuccess(fmt.Sprintf("Key '%s' exists", key))
		} else {
			printWarning(fmt.Sprintf("Key '%s' does not exist", key))
		}

	case "KEYS":
		keys := s.Keys()
		if len(keys) == 0 {
			printWarning("No keys stored")
			return
		}

		fmt.Printf("%sKeys (%d total):%s\n", colorBold, len(keys), colorReset)
		for i, key := range keys {
			fmt.Printf("  %s%d.%s %s\n", colorGray, i+1, colorReset, key)
		}

	case "LEN", "COUNT":
		count := s.Len()
		printInfo(fmt.Sprintf("Total keys: %d", count))

	case "COMMIT":
		s.Commit()
		printSuccess("OK (all writes flushed to disk)")

	case "CLEAR", "CLS":
		fmt.Print("\033[H\033[2J")
		printBanner()

	case "HELP", "?":
		printHelp()

	case "EXIT", "QUIT", "Q":
		printInfo("Goodbye!")
		os.Exit(0)

	default:
		printError(fmt.Sprintf("Unknown command: %s", cmd))
		fmt.Println("Type 'help' for available commands")
	}
}

func main() {
	// open WAL with 100ms flush interval and 10MB max segment size
	w, err := wal.Open("./walrus-data", 100*time.Millisecond, 10*1024*1024)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	s := store.New(w)

	// Recover existing data
	if err := s.Recover(); err != nil {
		log.Fatal(err)
	}

	// print banner
	printBanner()

	// show recovered data stats
	if s.Len() > 0 {
		printInfo(fmt.Sprintf("Recovered %d key(s) from disk", s.Len()))
		fmt.Println()
	}

	// setup readline with auto-complete
	completer := readline.NewPrefixCompleter(
		readline.PcItem("SET"),
		readline.PcItem("GET"),
		readline.PcItem("DELETE"),
		readline.PcItem("DEL"),
		readline.PcItem("HAS"),
		readline.PcItem("EXISTS"),
		readline.PcItem("KEYS"),
		readline.PcItem("LEN"),
		readline.PcItem("COUNT"),
		readline.PcItem("COMMIT"),
		readline.PcItem("CLEAR"),
		readline.PcItem("CLS"),
		readline.PcItem("HELP"),
		readline.PcItem("EXIT"),
		readline.PcItem("QUIT"),
	)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          colorPurple + "walrus> " + colorReset,
		HistoryFile:     ".walrus_history",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rl.Close()

	// REPL loop
	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				printInfo("Goodbye!")
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		handleCommand(s, parts)
	}

	// final commit before exit
	s.Commit()
}

package client

import (
	"bufio"
	"os"
)

func (f *fsm) writerLoop() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		message := scanner.Bytes()
		f.event <- event{evtUserMessage, message, nil}
	}
}

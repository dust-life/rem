package core

import "sync"

const (
	ErrCmdParseFailed  = 1
	ErrArgsParseFailed = 2
	ErrPrepareFailed   = 3
	ErrNoConsoleURL    = 4
	ErrCreateConsole   = 5
	ErrDialFailed      = 6
	ErrWouldBlock      = 7 // Non-blocking read/write: no data available
	ErrAgentNotFound   = 8 // rem_dial stop named an agent that is not registered
)

var Conns sync.Map

package main

import (
	"sync"
	"testing"

	"github.com/chainreactors/rem/agent"
	"github.com/chainreactors/rem/protocol/core"
)

func TestStopVerb(t *testing.T) {
	if id, ok := stopVerb([]string{"stop", "abc123"}); !ok || id != "abc123" {
		t.Fatalf("stop verb = %q, %v", id, ok)
	}
	for _, args := range [][]string{
		{"stop"},
		{"stop", "a", "b"},
		{"-c", "tcp://127.0.0.1:34996"},
		{"stopp", "abc123"},
		nil,
	} {
		if _, ok := stopVerb(args); ok {
			t.Fatalf("%v parsed as stop verb", args)
		}
	}
}

func TestStopAgentGo(t *testing.T) {
	agent.Agents.Map = &sync.Map{}
	t.Cleanup(func() { agent.Agents.Map = &sync.Map{} })

	a, err := agent.NewAgent(&agent.Config{Alias: "stop-target", Type: core.CLIENT})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	agent.Agents.Add(a)

	id, code := stopAgentGo("stop-target")
	if code != 0 || id != "stop-target" {
		t.Fatalf("stopAgentGo = %q, %d", id, code)
	}
	if !a.IsClosed() {
		t.Fatal("agent not closed by stop")
	}
	if agent.Agents.Exist("stop-target") {
		t.Fatal("agent still registered after stop")
	}

	if _, code := stopAgentGo("stop-ghost"); code != core.ErrAgentNotFound {
		t.Fatalf("ghost stop code = %d, want %d", code, core.ErrAgentNotFound)
	}
}

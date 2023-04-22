package raft

import (
	"fmt"
	"strings"
)

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

func (l *Log) at(idx int) *Entry {
	tmp := idx - l.Entries[0].Index
	return &l.Entries[tmp]
}

func (l *Log) truncate(idx int) {
	tmp := idx - l.Entries[0].Index
	l.Entries = l.Entries[:tmp]
}

func (l *Log) slice(idx int) []Entry {
	tmp := idx - l.Entries[0].Index
	return l.Entries[tmp:]
}

func (l *Log) len() int {
	return len(l.Entries) + l.Entries[0].Index - 1
}

func (l *Log) lastLog() *Entry {
	idx := len(l.Entries) - 1
	return &l.Entries[idx]
}

func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

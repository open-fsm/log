package log

import (
	"log"
	"fmt"
	"sync"
	"errors"
	"github.com/open-fsm/spec/proto"
)

var ErrNotReached = errors.New("vr.store: access entry at op-number is not reached")
var ErrArchived = errors.New("vr.store: access op-number is not reached due to archive")
var ErrOverflow = errors.New("vr.store: overflow")
var ErrUnavailable = errors.New("vr.store: requested entry at op-number is unavailable")

// operation log manager for view stamped replication
type Log struct {
	// has not been WAL
	unsafe

	store      *Store // store handler
	CommitNum  uint64 // current committed location
	AppliedNum uint64 // logs that have been applied
}

// create and initialize an object handler to manage logs
func New(store *Store) *Log {
	if store == nil {
		log.Panic("vr.oplog: stores must not be nil")
	}
	startOpNum, err := store.StartOpNum()
	if err != nil {
		panic(err)
	}
	lastOpNum, err := store.LastOpNum()
	if err != nil {
		panic(err)
	}
	opLog := &Log{
		store:      store,
		CommitNum:  startOpNum - 1,
		AppliedNum: startOpNum - 1,
	}
	opLog.offset = lastOpNum + 1
	return opLog
}

func (l *Log) mustInspectionOverflow(low, up uint64) {
	if low > up {
		log.Panicf("vr.oplog: invalid Subset %d > %d", low, up)
	}
	length := l.LastOpNum() - l.StartOpNum() + 1
	if low < l.StartOpNum() || up > l.StartOpNum()+length {
		log.Panicf("vr.oplog: Subset[%d,%d) overflow [%d,%d]", low, up, l.StartOpNum(), l.LastOpNum())
	}
}

// search part of log Entries
func (l *Log) Subset(low uint64, up uint64) []proto.Entry {
	l.mustInspectionOverflow(low, up)
	if low == up {
		return nil
	}
	var entries []proto.Entry
	if l.unsafe.offset > low {
		storedEntries, err := l.store.Subset(low, min(up, l.unsafe.offset))
		if err == ErrNotReached {
			log.Panicf("vr.oplog: Entries[%d:%d) is unavailable from stores", low, min(up, l.unsafe.offset))
		} else if err != nil {
			panic(err)
		}
		entries = storedEntries
	}
	if l.unsafe.offset < up {
		unsafe := l.unsafe.subset(max(low, l.unsafe.offset), up)
		if len(entries) > 0 {
			entries = append([]proto.Entry{}, entries...)
			entries = append(entries, unsafe...)
		} else {
			entries = unsafe
		}
	}
	return entries
}

func (l *Log) TryAppend(opNum, logNum, commitNum uint64, entries ...proto.Entry) (lastNewOpNum uint64, ok bool) {
	lastNewOpNum = opNum + uint64(len(entries))
	if l.CheckNum(opNum, logNum) {
		sc := l.scanCollision(entries)
		switch {
		case sc == 0:
		case sc <= l.CommitNum:
			log.Panicf("vr.oplog: entry %d collision with commit-number entry [commit-number(%d)]", sc, l.CommitNum)
		default:
			offset := opNum + 1
			l.append(entries[sc-offset:]...)
		}
		l.CommitTo(min(commitNum, lastNewOpNum))
		return lastNewOpNum, true
	}
	return 0, false
}

func (l *Log) append(entries ...proto.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastOpNum()
	}
	if ahead := entries[0].ViewStamp.OpNum - 1; ahead < l.CommitNum {
		log.Panicf("vr.oplog: ahead(%d) is out of range [commit-number(%d)]", ahead, l.CommitNum)
	}
	l.unsafe.truncateAndAppend(entries)
	return l.LastOpNum()
}

func (l *Log) scanCollision(entries []proto.Entry) uint64 {
	for _, entry := range entries {
		if !l.CheckNum(entry.ViewStamp.OpNum, entry.ViewStamp.ViewNum) {
			if entry.ViewStamp.OpNum <= l.LastOpNum() {
				log.Printf("vr.oplog: scan to collision at op-number %d [existing view-number: %d, collision view-number: %d]",
					entry.ViewStamp.OpNum, l.ViewNum(entry.ViewStamp.OpNum), entry.ViewStamp.ViewNum)
			}
			return entry.ViewStamp.OpNum
		}
	}
	return 0
}

func (l *Log) unsafeEntries() []proto.Entry {
	if len(l.unsafe.entries) == 0 {
		return nil
	}
	return l.unsafe.entries
}

func (l *Log) safeEntries() (entries []proto.Entry) {
	num := max(l.AppliedNum+1, l.StartOpNum())
	if l.CommitNum+1 > num {
		return l.Subset(num, l.CommitNum+1)
	}
	return nil
}

func (l *Log) AppliedState() (proto.AppliedState, error) {
	if l.unsafe.appliedState != nil {
		return *l.unsafe.appliedState, nil
	}
	return l.store.GetAppliedState()
}

func (l *Log) StartOpNum() uint64 {
	if num, ok := l.unsafe.tryGetStartOpNum(); ok {
		return num
	}
	num, err := l.store.StartOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (l *Log) LastOpNum() uint64 {
	if num, ok := l.unsafe.tryGetLastOpNum(); ok {
		return num
	}
	num, err := l.store.LastOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (l *Log) CommitTo(commitNum uint64) {
	if l.CommitNum < commitNum {
		if l.LastOpNum() < commitNum {
			log.Panicf("vr.oplog: to commit-number(%d) is out of range [last-op-number(%d)]", commitNum, l.LastOpNum())
		}
		l.CommitNum = commitNum
	}
}

func (l *Log) AppliedTo(num uint64) {
	if num == 0 {
		return
	}
	if l.CommitNum < num || num < l.AppliedNum {
		log.Panicf("vr.oplog: applied-number(%d) is out of range [prev-applied-number(%d), commit-number(%d)]", num, l.AppliedNum, l.CommitNum)
	}
	l.AppliedNum = num
}

func (l *Log) safeTo(on, vn uint64) {
	l.unsafe.safeTo(on, vn)
}

func (l *Log) safeAppliedStateTo(num uint64) {
	l.unsafe.safeAppliedStateTo(num)
}

func (l *Log) LastViewNum() uint64 {
	return l.ViewNum(l.LastOpNum())
}

func (l *Log) ViewNum(num uint64) uint64 {
	if num < (l.StartOpNum()-1) || num > l.LastOpNum() {
		return 0
	}
	if vn, ok := l.unsafe.tryGetViewNum(num); ok {
		return vn
	}
	svn, err := l.store.ViewNum(num)
	if err == nil {
		return svn
	}
	panic(err)
}

func (l *Log) Entries(num uint64) []proto.Entry {
	if num > l.LastOpNum() {
		return nil
	}
	return l.Subset(num, l.LastOpNum()+1)
}

func (l *Log) TotalEntries() []proto.Entry {
	return l.Entries(l.StartOpNum())
}

func (l *Log) isUpToDate(lastOpNum, viewNum uint64) bool {
	return viewNum > l.LastViewNum() || (viewNum == l.LastViewNum() && lastOpNum >= l.LastOpNum())
}

func (l *Log) CheckNum(on, vn uint64) bool {
	return l.ViewNum(on) == vn
}

func (l *Log) TryCommit(maxOpNum, viewNum uint64) bool {
	if maxOpNum > l.CommitNum && l.ViewNum(maxOpNum) == viewNum {
		l.CommitTo(maxOpNum)
		return true
	}
	return false
}

func (l *Log) recover(state proto.AppliedState) {
	log.Printf("vr.oplog: log [%s] starts to reset applied state [op-number: %d, view-number: %d]", l, state.Applied.ViewStamp.OpNum, state.Applied.ViewStamp.ViewNum)
	l.CommitNum = state.Applied.ViewStamp.OpNum
	l.unsafe.recover(state)
}

func (l *Log) String() string {
	return fmt.Sprintf("vr.oplog: commit-number=%d, applied-number=%d, unsafe.offsets=%d, len(unsafe.persistent_entries)=%d", l.CommitNum, l.AppliedNum, l.unsafe.offset, len(l.unsafe.entries))
}

// storage models of view stamped replication
type Store struct {
	sync.Mutex
	hardState    proto.HardState     // persistent state that has been stored to disk
	appliedState proto.AppliedState  // the state that has been used by the application layer
	entries      []proto.Entry       // used to manage newly added log Entries
}

func NewStore() *Store {
	return &Store{
		entries: make([]proto.Entry, 1),
	}
}

func (s *Store) SetHardState(hs proto.HardState) error {
	s.hardState = hs
	return nil
}

func (s *Store) SetAppliedState(as proto.AppliedState) error {
	s.Lock()
	defer s.Unlock()
	s.appliedState = as
	s.entries = []proto.Entry{{ViewStamp: as.Applied.ViewStamp}}
	return nil
}

func (s *Store) LoadConfigurationState() (proto.ConfigurationState, error) {
	return proto.ConfigurationState{}, nil
}

func (s *Store) LoadHardState() (proto.HardState, error) {
	return s.hardState, nil
}

func (s *Store) GetAppliedState() (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	return s.appliedState, nil
}

func (s *Store) Append(entries []proto.Entry) error {
	s.Lock()
	defer s.Unlock()
	if len(entries) == 0 {
		return nil
	}
	start := s.startOpNum()
	last := s.lastOpNum()
	if last < start {
		return nil
	}
	if start > entries[0].ViewStamp.OpNum {
		entries = entries[start-entries[0].ViewStamp.OpNum:]
	}
	offset := entries[0].ViewStamp.OpNum - s.startOpNum()
	if uint64(len(s.entries)) > offset {
		s.entries = append([]proto.Entry{}, s.entries[:offset]...)
		s.entries = append(s.entries, entries...)
	} else if uint64(len(s.entries)) == offset {
		s.entries = append(s.entries, entries...)
	} else {
		log.Panicf("vr.store: not found oplog entry [last: %d, append at: %d]",
			s.appliedState.Applied.ViewStamp.OpNum+uint64(len(s.entries)), entries[0].ViewStamp.OpNum)
	}
	return nil
}

func (s *Store) Subset(low, up uint64) ([]proto.Entry, error) {
	s.Lock()
	defer s.Unlock()
	offset := s.entries[0].ViewStamp.OpNum
	if low <= offset {
		return nil, ErrArchived
	}
	if up > s.lastOpNum()+1 {
		log.Panicf("vr.store: Entries up(%d) is overflow last-op-number(%d)", up, s.lastOpNum())
	}
	if len(s.entries) == 1 {
		return nil, ErrNotReached
	}
	return s.entries[low-offset:up-offset], nil
}

func (s *Store) ViewNum(num uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()
	vo := s.entries[0].ViewStamp.OpNum
	if num < vo {
		return 0, ErrArchived
	}
	if int(num-vo) >= len(s.entries) {
		return 0, ErrUnavailable
	}
	return s.entries[num-vo].ViewStamp.ViewNum, nil
}

func (s *Store) CommitNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.hardState.CommitNum, nil
}

func (s *Store) StartOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.startOpNum() + 1, nil
}

func (s *Store) startOpNum() uint64 {
	return s.entries[0].ViewStamp.OpNum
}

func (s *Store) LastOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.lastOpNum() - 1, nil
}

func (s *Store) lastOpNum() uint64 {
	return s.entries[0].ViewStamp.OpNum + uint64(len(s.entries))
}

func (s *Store) CreateAppliedState(num uint64, data []byte, rs *proto.ConfigurationState) (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	if num < s.appliedState.Applied.ViewStamp.OpNum {
		return proto.AppliedState{}, ErrOverflow
	}
	if num > s.lastOpNum() {
		log.Panicf("vr.store: applied-number state %d is overflow last op-number(%d)", num, s.lastOpNum())
	}
	s.appliedState.Applied.ViewStamp.OpNum = num
	s.appliedState.Applied.ViewStamp.ViewNum = s.entries[num-s.startOpNum()].ViewStamp.ViewNum
	s.appliedState.Data = data
	if rs != nil {
		s.appliedState.Applied.ConfigurationState = *rs
	}
	return s.appliedState, nil
}

func (s *Store) Archive(archiveNum uint64) error {
	s.Lock()
	defer s.Unlock()
	offset := s.startOpNum()
	if archiveNum <= offset {
		return ErrArchived
	}
	if archiveNum >= s.lastOpNum() {
		log.Panicf("vr.store: archive %d is overflow last op-number(%d)", archiveNum, offset+uint64(len(s.entries))-1)
	}
	num := archiveNum - offset
	entries := make([]proto.Entry, 1, 1+uint64(len(s.entries))-num)
	entries[0].ViewStamp.OpNum = s.entries[num].ViewStamp.OpNum
	entries[0].ViewStamp.ViewNum = s.entries[num].ViewStamp.ViewNum
	entries = append(entries, s.entries[num+1:]...)
	s.entries = entries
	return nil
}

// used to manage the intermediate state that has not
// yet been written to disk
type unsafe struct {
	offset       uint64               // the position of the last log currently loaded
	entries      []proto.Entry        // temporary logs that have not been wal
	appliedState *proto.AppliedState  // applied state handler
}

func (u *unsafe) subset(low uint64, up uint64) []proto.Entry {
	if low > up {
		log.Panicf("vr.unsafe: invalid unsafe.Subset %d > %d", low, up)
	}
	lower := u.offset
	upper := lower + uint64(len(u.entries))
	if low < lower || up > upper {
		log.Panicf("vr.unsafe: unsafe.Subset[%d,%d) overflow [%d,%d]", low, up, lower, upper)
	}
	return u.entries[low-u.offset : up-u.offset]
}

func (u *unsafe) truncateAndAppend(entries []proto.Entry) {
	ahead := entries[0].ViewStamp.OpNum - 1
	if ahead == u.offset+uint64(len(u.entries))-1 {
		u.entries = append(u.entries, entries...)
	} else if ahead < u.offset {
		log.Printf("vr.unsafe: replace the unsafe Entries from number %d", ahead+1)
		u.offset = ahead + 1
		u.entries = entries
	} else {
		log.Printf("vr.unsafe: truncate the unsafe Entries to number %d", ahead)
		u.entries = append([]proto.Entry{}, u.subset(u.offset, ahead+1)...)
		u.entries = append(u.entries, entries...)
	}
}

func (u *unsafe) tryGetStartOpNum() (uint64, bool) {
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum + 1, true
	}
	return 0, false
}

func (u *unsafe) tryGetLastOpNum() (uint64, bool) {
	if el := len(u.entries); el != 0 {
		return u.offset + uint64(el) - 1, true
	}
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum, true
	}
	return 0, false
}

func (u *unsafe) tryGetViewNum(num uint64) (uint64, bool) {
	if num < u.offset {
		if u.appliedState == nil {
			return 0, false
		}
		if applied := u.appliedState.Applied; applied.ViewStamp.OpNum == num {
			return applied.ViewStamp.ViewNum, true
		}
		return 0, false
	}
	last, ok := u.tryGetLastOpNum()
	if !ok {
		return 0, false
	}
	if num > last {
		return 0, false
	}
	return u.entries[num-u.offset].ViewStamp.ViewNum, true
}

func (u *unsafe) safeTo(on, vn uint64) {
	this, ok := u.tryGetViewNum(on)
	if !ok {
		return
	}
	if this == vn && on >= u.offset {
		u.entries = u.entries[on+1-u.offset:]
		u.offset = on + 1
	}
}

func (u *unsafe) safeAppliedStateTo(num uint64) {
	if u.appliedState != nil && u.appliedState.Applied.ViewStamp.OpNum == num {
		u.appliedState = nil
	}
}

func (u *unsafe) recover(state proto.AppliedState) {
	u.entries = nil
	u.offset = state.Applied.ViewStamp.OpNum + 1
	u.appliedState = &state
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
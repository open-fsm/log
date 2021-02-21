package log

import (
	logger "log"
	"fmt"
	"sync"
	"errors"
	"github.com/open-fsm/spec/proto"
)

var ErrNotReached = errors.New("log.store: access entry at op-number is not reached")
var ErrArchived = errors.New("log.store: access op-number is not reached due to archive")
var ErrOverflow = errors.New("log.store: overflow")
var ErrUnavailable = errors.New("log.store: requested entry at op-number is unavailable")

type Log interface {
	Init(u Unsafe, commitNum  uint64)
	Subset(low uint64, up uint64) []proto.Entry
	TryAppend(opNum, loggerNum, commitNum uint64, entries ...proto.Entry) (lastNewOpNum uint64, ok bool)
	Append(entries ...proto.Entry) uint64
	UnsafeEntries() []proto.Entry
	UnsafeAppliedState() *proto.AppliedState
	SafeEntries() (entries []proto.Entry)
	AppliedState() (proto.AppliedState, error)
	StartOpNum() uint64
	LastOpNum() uint64
	CommitTo(commitNum uint64)
	AppliedTo(num uint64)
	Entries(num uint64) []proto.Entry
	SafeTo(on, vn uint64)
	TryCommit(maxOpNum, viewNum uint64) bool
	TotalEntries() []proto.Entry
	CheckNum(on, vn uint64) bool
	ViewNum(num uint64) uint64
	Committed() uint64
	SetCommitted(uint64)
	Applied() uint64
	Recover(state proto.AppliedState)
	LastViewNum() uint64
	SafeAppliedStateTo(num uint64)
}

// operation logger manager for view stamped replication
type log struct {
	// has not been WAL
	Unsafe

	Store      *Store // Store handler
	CommitNum  uint64 // current committed location
	AppliedNum uint64 // loggers that have been applied
}

// create and initialize an object handler to manage loggers
func New(store *Store) Log {
	if store == nil {
		logger.Panic("log: stores must not be nil")
	}
	startOpNum, err := store.StartOpNum()
	if err != nil {
		panic(err)
	}
	lastOpNum, err := store.LastOpNum()
	if err != nil {
		panic(err)
	}
	log := &log{
		Store:      store,
		CommitNum:  startOpNum - 1,
		AppliedNum: startOpNum - 1,
	}
	log.Offset = lastOpNum + 1
	return log
}

func (l *log) Init(u Unsafe, commitNum  uint64) {
	l.Unsafe = u
	l.CommitNum = commitNum
}

func (l *log) mustInspectionOverflow(low, up uint64) {
	if low > up {
		logger.Panicf("log: invalid Subset %d > %d", low, up)
	}
	length := l.LastOpNum() - l.StartOpNum() + 1
	if low < l.StartOpNum() || up > l.StartOpNum()+length {
		logger.Panicf("log: subset[%d,%d) overflow [%d,%d]", low, up, l.StartOpNum(), l.LastOpNum())
	}
}

func (l *log) Subset(low uint64, up uint64) []proto.Entry {
	l.mustInspectionOverflow(low, up)
	if low == up {
		return nil
	}
	var entries []proto.Entry
	if l.Unsafe.Offset > low {
		storedEntries, err := l.Store.Subset(low, min(up, l.Unsafe.Offset))
		if err == ErrNotReached {
			logger.Panicf("log: Entries[%d:%d) is unavailable from stores", low, min(up, l.Unsafe.Offset))
		} else if err != nil {
			panic(err)
		}
		entries = storedEntries
	}
	if l.Unsafe.Offset < up {
		unsafe := l.Unsafe.subset(max(low, l.Unsafe.Offset), up)
		if len(entries) > 0 {
			entries = append([]proto.Entry{}, entries...)
			entries = append(entries, unsafe...)
		} else {
			entries = unsafe
		}
	}
	return entries
}

func (l *log) TryAppend(opNum, loggerNum, commitNum uint64, entries ...proto.Entry) (lastNewOpNum uint64, ok bool) {
	lastNewOpNum = opNum + uint64(len(entries))
	if l.CheckNum(opNum, loggerNum) {
		sc := l.scanCollision(entries)
		switch {
		case sc == 0:
		case sc <= l.CommitNum:
			logger.Panicf("log: entry %d collision with commit-number entry [commit-number(%d)]", sc, l.CommitNum)
		default:
			offset := opNum + 1
			l.Append(entries[sc-offset:]...)
		}
		l.CommitTo(min(commitNum, lastNewOpNum))
		return lastNewOpNum, true
	}
	return 0, false
}

func (l *log) Append(entries ...proto.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastOpNum()
	}
	if ahead := entries[0].ViewStamp.OpNum - 1; ahead < l.CommitNum {
		logger.Panicf("log: ahead(%d) is out of range [commit-number(%d)]", ahead, l.CommitNum)
	}
	l.Unsafe.truncateAndAppend(entries)
	return l.LastOpNum()
}

func (l *log) scanCollision(entries []proto.Entry) uint64 {
	for _, entry := range entries {
		if !l.CheckNum(entry.ViewStamp.OpNum, entry.ViewStamp.ViewNum) {
			if entry.ViewStamp.OpNum <= l.LastOpNum() {
				logger.Printf("log: scan to collision at op-number %d [existing view-number: %d, collision view-number: %d]",
					entry.ViewStamp.OpNum, l.ViewNum(entry.ViewStamp.OpNum), entry.ViewStamp.ViewNum)
			}
			return entry.ViewStamp.OpNum
		}
	}
	return 0
}

func (l *log) UnsafeEntries() []proto.Entry {
	if len(l.Unsafe.entries) == 0 {
		return nil
	}
	return l.Unsafe.entries
}

func (l *log) UnsafeAppliedState() *proto.AppliedState {
	return l.Unsafe.appliedState
}

func (l *log) SafeEntries() (entries []proto.Entry) {
	num := max(l.AppliedNum+1, l.StartOpNum())
	if l.CommitNum+1 > num {
		return l.Subset(num, l.CommitNum+1)
	}
	return nil
}

func (l *log) AppliedState() (proto.AppliedState, error) {
	if l.Unsafe.appliedState != nil {
		return *l.Unsafe.appliedState, nil
	}
	return l.Store.GetAppliedState()
}

func (l *log) StartOpNum() uint64 {
	if num, ok := l.Unsafe.tryGetStartOpNum(); ok {
		return num
	}
	num, err := l.Store.StartOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (l *log) LastOpNum() uint64 {
	if num, ok := l.Unsafe.tryGetLastOpNum(); ok {
		return num
	}
	num, err := l.Store.LastOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (l *log) CommitTo(commitNum uint64) {
	if l.CommitNum < commitNum {
		if l.LastOpNum() < commitNum {
			logger.Panicf("log: to commit-number(%d) is out of range [last-op-number(%d)]", commitNum, l.LastOpNum())
		}
		l.CommitNum = commitNum
	}
}

func (l *log) AppliedTo(num uint64) {
	if num == 0 {
		return
	}
	if l.CommitNum < num || num < l.AppliedNum {
		logger.Panicf("log: applied-number(%d) is out of range [prev-applied-number(%d), commit-number(%d)]", num, l.AppliedNum, l.CommitNum)
	}
	l.AppliedNum = num
}

func (l *log) SafeTo(on, vn uint64) {
	l.Unsafe.safeTo(on, vn)
}

func (l *log) SafeAppliedStateTo(num uint64) {
	l.Unsafe.safeAppliedStateTo(num)
}

func (l *log) LastViewNum() uint64 {
	return l.ViewNum(l.LastOpNum())
}

func (l *log) ViewNum(num uint64) uint64 {
	if num < (l.StartOpNum()-1) || num > l.LastOpNum() {
		return 0
	}
	if vn, ok := l.Unsafe.tryGetViewNum(num); ok {
		return vn
	}
	svn, err := l.Store.ViewNum(num)
	if err == nil {
		return svn
	}
	panic(err)
}

func (l *log) Entries(num uint64) []proto.Entry {
	if num > l.LastOpNum() {
		return nil
	}
	return l.Subset(num, l.LastOpNum()+1)
}

func (l *log) TotalEntries() []proto.Entry {
	return l.Entries(l.StartOpNum())
}

func (l *log) isUpToDate(lastOpNum, viewNum uint64) bool {
	return viewNum > l.LastViewNum() || (viewNum == l.LastViewNum() && lastOpNum >= l.LastOpNum())
}

func (l *log) CheckNum(on, vn uint64) bool {
	return l.ViewNum(on) == vn
}

func (l *log) TryCommit(maxOpNum, viewNum uint64) bool {
	if maxOpNum > l.CommitNum && l.ViewNum(maxOpNum) == viewNum {
		l.CommitTo(maxOpNum)
		return true
	}
	return false
}

func (l *log) Recover(state proto.AppliedState) {
	logger.Printf("log: logger [%s] starts to reset applied state [op-number: %d, view-number: %d]", l, state.Applied.ViewStamp.OpNum, state.Applied.ViewStamp.ViewNum)
	l.CommitNum = state.Applied.ViewStamp.OpNum
	l.Unsafe.recover(state)
}

func (l *log) Committed() uint64 {
	return l.CommitNum
}

func (l *log) SetCommitted(num uint64) {
	l.CommitNum = num
}

func (l *log) Applied() uint64 {
	return l.AppliedNum
}

func (l *log) String() string {
	return fmt.Sprintf("log: commit-number=%d, applied-number=%d, Unsafe.offsets=%d, len(Unsafe.persistent_entries)=%d", l.CommitNum, l.AppliedNum, l.Unsafe.Offset, len(l.Unsafe.entries))
}

// storage models of view stamped replication
type Store struct {
	sync.Mutex
	HardState    proto.HardState    // persistent state that has been stored to disk
	appliedState proto.AppliedState // the state that has been used by the application layer
	Entries      []proto.Entry      // used to manage newly added logger Entries
}

func NewStore() *Store {
	return &Store{
		Entries: make([]proto.Entry, 1),
	}
}

func (s *Store) SetHardState(hs proto.HardState) error {
	s.HardState = hs
	return nil
}

func (s *Store) SetAppliedState(as proto.AppliedState) error {
	s.Lock()
	defer s.Unlock()
	s.appliedState = as
	s.Entries = []proto.Entry{{ViewStamp: as.Applied.ViewStamp}}
	return nil
}

func (s *Store) LoadConfigurationState() (proto.ConfigurationState, error) {
	return proto.ConfigurationState{}, nil
}

func (s *Store) LoadHardState() (proto.HardState, error) {
	return s.HardState, nil
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
	if uint64(len(s.Entries)) > offset {
		s.Entries = append([]proto.Entry{}, s.Entries[:offset]...)
		s.Entries = append(s.Entries, entries...)
	} else if uint64(len(s.Entries)) == offset {
		s.Entries = append(s.Entries, entries...)
	} else {
		logger.Panicf("log.store: not found oplogger entry [last: %d, Append at: %d]",
			s.appliedState.Applied.ViewStamp.OpNum+uint64(len(s.Entries)), entries[0].ViewStamp.OpNum)
	}
	return nil
}

func (s *Store) Subset(low, up uint64) ([]proto.Entry, error) {
	s.Lock()
	defer s.Unlock()
	offset := s.Entries[0].ViewStamp.OpNum
	if low <= offset {
		return nil, ErrArchived
	}
	if up > s.lastOpNum()+1 {
		logger.Panicf("log.store: Entries up(%d) is overflow last-op-number(%d)", up, s.lastOpNum())
	}
	if len(s.Entries) == 1 {
		return nil, ErrNotReached
	}
	return s.Entries[low-offset:up-offset], nil
}

func (s *Store) ViewNum(num uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()
	vo := s.Entries[0].ViewStamp.OpNum
	if num < vo {
		return 0, ErrArchived
	}
	if int(num-vo) >= len(s.Entries) {
		return 0, ErrUnavailable
	}
	return s.Entries[num-vo].ViewStamp.ViewNum, nil
}

func (s *Store) CommitNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.HardState.CommitNum, nil
}

func (s *Store) StartOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.startOpNum() + 1, nil
}

func (s *Store) startOpNum() uint64 {
	return s.Entries[0].ViewStamp.OpNum
}

func (s *Store) LastOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.lastOpNum() - 1, nil
}

func (s *Store) lastOpNum() uint64 {
	return s.Entries[0].ViewStamp.OpNum + uint64(len(s.Entries))
}

func (s *Store) CreateAppliedState(num uint64, data []byte, rs *proto.ConfigurationState) (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	if num < s.appliedState.Applied.ViewStamp.OpNum {
		return proto.AppliedState{}, ErrOverflow
	}
	if num > s.lastOpNum() {
		logger.Panicf("log.store: applied-number state %d is overflow last op-number(%d)", num, s.lastOpNum())
	}
	s.appliedState.Applied.ViewStamp.OpNum = num
	s.appliedState.Applied.ViewStamp.ViewNum = s.Entries[num-s.startOpNum()].ViewStamp.ViewNum
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
		logger.Panicf("log.store: archive %d is overflow last op-number(%d)", archiveNum, offset+uint64(len(s.Entries))-1)
	}
	num := archiveNum - offset
	entries := make([]proto.Entry, 1, 1+uint64(len(s.Entries))-num)
	entries[0].ViewStamp.OpNum = s.Entries[num].ViewStamp.OpNum
	entries[0].ViewStamp.ViewNum = s.Entries[num].ViewStamp.ViewNum
	entries = append(entries, s.Entries[num+1:]...)
	s.Entries = entries
	return nil
}

// used to manage the intermediate state that has not
// yet been written to disk
type Unsafe struct {
	Offset       uint64              // the position of the last logger currently loaded
	entries      []proto.Entry       // temporary loggers that have not been wal
	appliedState *proto.AppliedState // applied state handler
}

func (u *Unsafe) subset(low uint64, up uint64) []proto.Entry {
	if low > up {
		logger.Panicf("log.unsafe: invalid Unsafe.Subset %d > %d", low, up)
	}
	lower := u.Offset
	upper := lower + uint64(len(u.entries))
	if low < lower || up > upper {
		logger.Panicf("log.unsafe: Unsafe.Subset[%d,%d) overflow [%d,%d]", low, up, lower, upper)
	}
	return u.entries[low-u.Offset: up-u.Offset]
}

func (u *Unsafe) truncateAndAppend(entries []proto.Entry) {
	ahead := entries[0].ViewStamp.OpNum - 1
	if ahead == u.Offset+uint64(len(u.entries))-1 {
		u.entries = append(u.entries, entries...)
	} else if ahead < u.Offset {
		logger.Printf("log.unsafe: replace the Unsafe Entries from number %d", ahead+1)
		u.Offset = ahead + 1
		u.entries = entries
	} else {
		logger.Printf("log.unsafe: truncate the Unsafe Entries to number %d", ahead)
		u.entries = append([]proto.Entry{}, u.subset(u.Offset, ahead+1)...)
		u.entries = append(u.entries, entries...)
	}
}

func (u *Unsafe) tryGetStartOpNum() (uint64, bool) {
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum + 1, true
	}
	return 0, false
}

func (u *Unsafe) tryGetLastOpNum() (uint64, bool) {
	if el := len(u.entries); el != 0 {
		return u.Offset + uint64(el) - 1, true
	}
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum, true
	}
	return 0, false
}

func (u *Unsafe) tryGetViewNum(num uint64) (uint64, bool) {
	if num < u.Offset {
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
	return u.entries[num-u.Offset].ViewStamp.ViewNum, true
}

func (u *Unsafe) safeTo(on, vn uint64) {
	this, ok := u.tryGetViewNum(on)
	if !ok {
		return
	}
	if this == vn && on >= u.Offset {
		u.entries = u.entries[on+1-u.Offset:]
		u.Offset = on + 1
	}
}

func (u *Unsafe) safeAppliedStateTo(num uint64) {
	if u.appliedState != nil && u.appliedState.Applied.ViewStamp.OpNum == num {
		u.appliedState = nil
	}
}

func (u *Unsafe) recover(state proto.AppliedState) {
	u.entries = nil
	u.Offset = state.Applied.ViewStamp.OpNum + 1
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
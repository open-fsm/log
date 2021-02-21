package log

import (
	"reflect"
	"testing"
	"github.com/open-fsm/spec"
	"github.com/open-fsm/spec/proto"
)

func initViewStampCase() {
	spec.InitViewStampCase()
}

func TestAppend(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp: spec.V1o1}, {ViewStamp: spec.V2o2}}
	cases := []struct {
		entries    []proto.Entry
		expOpNum   uint64
		expEntries []proto.Entry
		expUnsafe  uint64
	}{
		{[]proto.Entry{},2,[]proto.Entry{{ViewStamp: spec.V1o1}, {ViewStamp: spec.V2o2}},3 },
		{[]proto.Entry{{ViewStamp: spec.V2o3}},3,[]proto.Entry{{ViewStamp: spec.V1o1}, {ViewStamp: spec.V2o2}, {ViewStamp: spec.V2o3}},3 },
		{[]proto.Entry{{ViewStamp: spec.V2o1}},1,[]proto.Entry{{ViewStamp: spec.V2o1}},1 },
		{[]proto.Entry{{ViewStamp: spec.V3o2}, {ViewStamp: spec.V3o3}},3,[]proto.Entry{{ViewStamp: spec.V1o1}, {ViewStamp: spec.V3o2}, {ViewStamp: spec.V3o3}}, 2 },
	}
	for i, test := range cases {
		store := NewStore()
		store.Append(prevEntries)
		log := New(store)
		opNum := log.Append(test.entries...)
		if opNum != test.expOpNum {
			t.Errorf("#%d: last op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
		if rv := log.Entries(1); !reflect.DeepEqual(rv, test.expEntries) {
			t.Errorf("#%d: log entries = %+v, expected %+v", i, rv, test.expEntries)
		}
		if v := log.Unsafe.Offset; v != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, v, test.expUnsafe)
		}
	}
}

func TestTryAppend(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}}
	lastOpNum := uint64(3)
	lastViewNum := uint64(3)
	commitNum := uint64(1)
	cases := []struct {
		logViewNum   uint64
		opNum        uint64
		commitNum    uint64
		entries      []proto.Entry
		expLastOpNum uint64
		expAppend    bool
		expCommitNum uint64
		expPanic     bool
	}{
		{
			lastViewNum - 1,lastOpNum,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 1, ViewNum: 4}}},
			0,false,commitNum,false,
		},
		{
			lastViewNum,lastOpNum + 1,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 2, ViewNum: 4}}},
			0,false,commitNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum,nil,
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 1,nil,
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum - 1,nil,
			lastOpNum,true,lastOpNum - 1,false,
		},
		{
			lastViewNum,lastOpNum,0,nil,
			lastOpNum,true,commitNum,false,
		},
		{
			0,0,lastOpNum,nil,
			0,true,commitNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 1, ViewNum: 4}}},
			lastOpNum + 1,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 1,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 1, ViewNum: 4}}},
			lastOpNum + 1,true,lastOpNum + 1,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 2,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 1, ViewNum: 4}}},
			lastOpNum + 1,true,lastOpNum + 1,false,
		},
		{
			lastViewNum,lastOpNum, lastOpNum + 2, []proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 1, ViewNum: 4}}, {ViewStamp:proto.ViewStamp{OpNum: lastOpNum + 2, ViewNum: 4}}},
			lastOpNum + 2,true, lastOpNum + 2, false,
		},
		{
			lastViewNum - 1,lastOpNum - 1,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum, ViewNum: 4}}},
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum - 2, lastOpNum - 2, lastOpNum, []proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum - 1, ViewNum: 4}}},
			lastOpNum - 1, true, lastOpNum - 1, false,
		},
		{
			lastViewNum - 3,lastOpNum - 3,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum - 2, ViewNum: 4}}},
			lastOpNum - 2,true,lastOpNum - 2,true,
		},
		{
			lastViewNum - 2,lastOpNum - 2,lastOpNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: lastOpNum - 1, ViewNum: 4}}, {ViewStamp:proto.ViewStamp{OpNum: lastOpNum, ViewNum: 4}}},
			lastOpNum,true,lastOpNum,false,
		},
	}
	for i, test := range cases {
		log := New(NewStore())
		log.Append(prevEntries...)
		log.CommitNum = commitNum
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expPanic != true {
						t.Errorf("%d: panic = %v, expected %v", i, true, test.expPanic)
					}
				}
			}()
			rvLastOpNum, rvAppend := log.TryAppend(test.opNum, test.logViewNum, test.commitNum, test.entries...)
			vCommitNum := log.CommitNum
			if rvLastOpNum != test.expLastOpNum {
				t.Errorf("#%d: last op-number = %d, expected %d", i, rvLastOpNum, test.expLastOpNum)
			}
			if rvAppend != test.expAppend {
				t.Errorf("#%d: append = %v, expected %v", i, rvAppend, test.expAppend)
			}
			if vCommitNum != test.expCommitNum {
				t.Errorf("#%d: commit-number = %d, expected %d", i, vCommitNum, test.expCommitNum)
			}
			if rvAppend && len(test.entries) != 0 {
				rvEntries := log.subset(log.LastOpNum()-uint64(len(test.entries))+1, log.LastOpNum()+1)
				if !reflect.DeepEqual(test.entries, rvEntries) {
					t.Errorf("%d: appended entries = %v, expected %v", i, rvEntries, test.entries)
				}
			}
		}()
	}
}

func TestNextEntries(t *testing.T) {
	appliedState := proto.AppliedState{
		Applied: proto.Applied{ViewStamp: spec.V1o3},
	}
	entries := []proto.Entry{
		{ViewStamp:spec.V1o4},
		{ViewStamp:spec.V1o5},
		{ViewStamp:spec.V1o6},
	}
	cases := []struct {
		appliedNum uint64
		expEntries []proto.Entry
	}{
		{0,entries[:2]},
		{3,entries[:2]},
		{4,entries[1:2]},
		{5,nil},
	}
	for i, test := range cases {
		store := NewStore()
		store.SetAppliedState(appliedState)
		log := New(store)
		log.Append(entries...)
		log.TryCommit(5, 1)
		log.AppliedTo(test.appliedNum)
		entryList := log.SafeEntries()
		if !reflect.DeepEqual(entryList, test.expEntries) {
			t.Errorf("#%d: entry list = %+v, expected %+v", i, entryList, test.expEntries)
		}
	}
}

func TestUnsafeEntries(t *testing.T) {
	prevEntries := []proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}}
	cases := []struct {
		unsafe     uint64
		expEntries []proto.Entry
	}{
		{3, nil},
		{1, prevEntries},
	}
	for i, test := range cases {
		store := NewStore()
		store.Append(prevEntries[:test.unsafe-1])
		log := New(store)
		log.Append(prevEntries[test.unsafe-1:]...)
		entries := log.UnsafeEntries()
		if l := len(entries); l > 0 {
			log.SafeTo(entries[l-1].ViewStamp.OpNum, entries[l-i].ViewStamp.ViewNum)
		}
		if !reflect.DeepEqual(entries, test.expEntries) {
			t.Errorf("#%d: unsafe entries = %+v, expected %+v", i, entries, test.expEntries)
		}
		w := prevEntries[len(prevEntries)-1].ViewStamp.OpNum + 1
		if g := log.Unsafe.Offset; g != w {
			t.Errorf("#%d: unsafe = %d, expected %d", i, g, w)
		}
	}
}

func TestCommitTo(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}}
	commitNum := uint64(2)
	cases := []struct {
		commitNum    uint64
		expCommitNum uint64
		expPanic     bool
	}{
		{3,3,false},
		{1,2,false},
		{4,0,true},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expPanic != true {
						t.Errorf("%d: panic = %v, expected %v", i, true, test.expPanic)
					}
				}
			}()
			log := New(NewStore())
			log.Append(prevEntries...)
			log.CommitNum = commitNum
			log.CommitTo(test.commitNum)
			if log.CommitNum != test.expCommitNum {
				t.Errorf("#%d: commit-number = %d, expected %d", i, log.CommitNum, test.expCommitNum)
			}
		}()
	}
}

func TestSafeTo(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		safeOpNum   uint64
		safeViewNum uint64
		expUnsafe   uint64
	}{
		{1,1,2},
		{2,2,3},
		{2,1,1},
		{3,1,1},
	}
	for i, test := range cases {
		log := New(NewStore())
		log.Append([]proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}}...)
		log.SafeTo(test.safeOpNum, test.safeViewNum)
		if log.Unsafe.Offset != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, log.Unsafe.Offset, test.expUnsafe)
		}
	}
}

func TestSafeToWithAppliedState(t *testing.T) {
	appliedStateOpNum, appliedStateViewNum := uint64(5), uint64(2)
	appliedState := proto.AppliedState{Applied: proto.Applied{ViewStamp: proto.ViewStamp{OpNum: appliedStateOpNum, ViewNum: appliedStateViewNum}}}
	cases := []struct {
		safeOpNum   uint64
		safeViewNum uint64
		newEntries  []proto.Entry
		expUnsafe   uint64
	}{
		{appliedStateOpNum + 1,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum,[]proto.Entry{{ViewStamp: proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 2},
		{appliedStateOpNum,appliedStateViewNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum + 1,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum + 1,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum + 1,[]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}}},appliedStateOpNum + 1},
	}
	for i, test := range cases {
		store := NewStore()
		store.SetAppliedState(appliedState)
		log := New(store)
		log.Append(test.newEntries...)
		log.SafeTo(test.safeOpNum, test.safeViewNum)
		if log.Unsafe.Offset != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, log.Unsafe.Offset, test.expUnsafe)
		}
	}
}

func TestArchive(t *testing.T) {
	cases := []struct {
		lastOpNum  uint64
		archive    []uint64
		expOverage []int
		expAllow   bool
	}{
		{1000,[]uint64{1001},[]int{-1},false},
		{1000,[]uint64{300, 500, 800, 900},[]int{700, 500, 200, 100},true},
		{1000,[]uint64{300, 299},[]int{700, -1},false},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expAllow == true {
						t.Errorf("case %d: allow = %v, expected %v: %v", i, false, true, r)
					}
				}
			}()
			store := NewStore()
			for num := uint64(1); num <= test.lastOpNum; num++ {
				store.Append([]proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: num}}})
			}
			log := New(store)
			log.TryCommit(test.lastOpNum, 0)
			log.AppliedTo(log.CommitNum)
			for j := 0; j < len(test.archive); j++ {
				err := store.Archive(test.archive[j])
				if err != nil {
					if test.expAllow {
						t.Errorf("case %d.%d allow = %t, expected %t", i, j, false, test.expAllow)
					}
					continue
				}
				if len(log.TotalEntries()) != test.expOverage[j] {
					t.Errorf("case %d.%d len = %d, expected %d", i, j, len(log.TotalEntries()), test.expOverage[j])
				}
			}
		}()
	}
}

func TestArchiveDisorder(t *testing.T) {
	var i uint64
	lastOpNum := uint64(1000)
	unsafeOpNum := uint64(750)
	lastViewNum := lastOpNum
	store := NewStore()
	for i = 1; i <= unsafeOpNum; i++ {
		store.Append([]proto.Entry{{ViewStamp:proto.ViewStamp{ViewNum: uint64(i), OpNum: uint64(i)}}})
	}
	log := New(store)
	for i = unsafeOpNum; i < lastOpNum; i++ {
		log.Append(proto.Entry{ViewStamp:proto.ViewStamp{ViewNum: uint64(i + 1), OpNum: uint64(i + 1)}})
	}
	ok := log.TryCommit(lastOpNum, lastViewNum)
	if !ok {
		t.Fatalf("try commit returned false")
	}
	log.AppliedTo(log.CommitNum)
	offset := uint64(500)
	store.Archive(offset)
	if log.LastOpNum() != lastOpNum {
		t.Errorf("last op-number = %d, expected %d", log.LastOpNum(), lastOpNum)
	}
	for j := offset; j <= log.LastOpNum(); j++ {
		if log.ViewNum(j) != j {
			t.Errorf("view-number(%d) = %d, expected %d", j, log.ViewNum(j), j)
		}
	}
	for j := offset; j <= log.LastOpNum(); j++ {
		if !log.CheckNum(j, j) {
			t.Errorf("check view-number(%d) = false, expected true", j)
		}
	}
	unsafeEntries := log.UnsafeEntries()
	if l := len(unsafeEntries); l != 250 {
		t.Errorf("unsafe entries length = %d, expected = %d", l, 250)
	}
	if unsafeEntries[0].ViewStamp.OpNum != 751 {
		t.Errorf("op-number = %d, expected = %d", unsafeEntries[0].ViewStamp.OpNum, 751)
	}
	prev := log.LastOpNum()
	log.Append(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: log.LastOpNum() + 1, ViewNum: log.LastOpNum() + 1}})
	if log.LastOpNum() != prev+1 {
		t.Errorf("last op-number = %d, expected = %d", log.LastOpNum(), prev+1)
	}
	entries := log.Entries(log.LastOpNum())
	if len(entries) != 1 {
		t.Errorf("entries length = %d, expected = %d", len(entries), 1)
	}
}

func TestLogStore(t *testing.T) {
	opNum, viewNum := uint64(1000), uint64(1000)
	applied := proto.Applied{ViewStamp:proto.ViewStamp{OpNum: opNum, ViewNum: viewNum}}
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied: applied})
	log := New(store)
	if len(log.TotalEntries()) != 0 {
		t.Errorf("len = %d, expected 0", len(log.TotalEntries()))
	}
	if log.CommitNum != opNum {
		t.Errorf("commit-number = %d, expected %d", log.CommitNum, opNum)
	}
	if log.Unsafe.Offset != opNum+1 {
		t.Errorf("unsafe = %v, expected %d", log.Unsafe, opNum+1)
	}
	if log.ViewNum(opNum) != viewNum {
		t.Errorf("view-number = %d, expected %d", log.ViewNum(opNum), viewNum)
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{ViewStamp:proto.ViewStamp{OpNum: offset}}})
	log := New(store)
	for i := uint64(1); i <= num; i++ {
		log.Append(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: i + offset}})
	}
	start := offset + 1
	cases := []struct {
		low, up  uint64
		expPanic bool
	}{
		{start - 2,start + 1,true },
		{start - 1,start + 1,true },
		{start,start,false },
		{start + num/2,start + num/2,false },
		{start + num - 1,start + num - 1,false },
		{start + num,start + num,false },
		{start + num,start + num + 1,true },
		{start + num + 1,start + num + 1,true },
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !test.expPanic {
						t.Errorf("%d: panic = %v, expected %v: %v", i, true, false, r)
					}
				}
			}()
			log.mustInspectionOverflow(test.low, test.up)
			if test.expPanic {
				t.Errorf("%d: panic = %v, expected %v", i, false, true)
			}
		}()
	}
}

func TestViewNum(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{ViewStamp:proto.ViewStamp{OpNum: offset, ViewNum: 1}}})
	l := New(store)
	for i = 1; i < num; i++ {
		l.Append(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: offset + i, ViewNum: i}})
	}
	cases := []struct {
		opNum uint64
		exp   uint64
	}{
		{offset - 1,0},
		{offset, 1},
		{offset + num/2,num / 2},
		{offset + num - 1,num - 1},
		{offset + num,0},
	}
	for j, test := range cases {
		viewNum := l.ViewNum(test.opNum)
		if !reflect.DeepEqual(viewNum, test.exp) {
			t.Errorf("#%d: at = %d, expected %d", j, viewNum, test.exp)
		}
	}
}

func TestViewNumWithUnsafeAppliedState(t *testing.T) {
	storeAppliedStateOpNum := uint64(100)
	unsafeAppliedStateOpNum := storeAppliedStateOpNum + 5
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{ViewStamp:proto.ViewStamp{OpNum: storeAppliedStateOpNum, ViewNum: 1}}})
	log := New(store)
	log.Recover(proto.AppliedState{Applied:proto.Applied{ViewStamp:proto.ViewStamp{OpNum: unsafeAppliedStateOpNum, ViewNum: 1}}})
	cases := []struct {
		opNum uint64
		exp   uint64
	}{
		{storeAppliedStateOpNum,0},
		{storeAppliedStateOpNum + 1,0},
		{unsafeAppliedStateOpNum - 1,0},
		{unsafeAppliedStateOpNum,1},
	}
	for i, test := range cases {
		viewNum := log.ViewNum(test.opNum)
		if !reflect.DeepEqual(viewNum, test.exp) {
			t.Errorf("#%d: at = %d, expected %d", i, viewNum, test.exp)
		}
	}
}

func TestSeek(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{ViewStamp:proto.ViewStamp{OpNum: offset}}})
	log := New(store)
	for i = 1; i < num; i++ {
		log.Append(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: offset + i, ViewNum: offset + i}})
	}
	cases := []struct {
		from     uint64
		to       uint64
		exp      []proto.Entry
		expPanic bool
	}{
		{offset - 1, offset + 1, nil, true},
		{offset, offset + 1, nil, true},
		{offset + num/2, offset + num/2 + 1, []proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: offset + num/2, ViewNum: offset + num/2}}}, false},
		{offset + num - 1, offset + num, []proto.Entry{{ViewStamp:proto.ViewStamp{OpNum: offset + num - 1, ViewNum: offset + num - 1}}}, false},
		{offset + num, offset + num + 1, nil, true},
	}
	for j, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !test.expPanic {
						t.Errorf("%d: panic = %v, expected %v: %v", j, true, false, r)
					}
				}
			}()
			rv := log.Subset(test.from, test.to)
			if !reflect.DeepEqual(rv, test.exp) {
				t.Errorf("#%d: from %d to %d = %v, expected %v", j, test.from, test.to, rv, test.exp)
			}
		}()
	}
}

func TestScanCollision(t *testing.T) {
	initViewStampCase()
	presetEntries := []proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}}
	cases := []struct {
		entries      []proto.Entry
		expCollision uint64
	}{
		{[]proto.Entry{},0},
		{[]proto.Entry{},0},
		{[]proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}},0},
		{[]proto.Entry{{ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}}, 0},
		{[]proto.Entry{{ViewStamp:spec.V3o3}}, 0},
		{[]proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}, {ViewStamp:spec.V4o4}, {ViewStamp:spec.V4o5}},4},
		{[]proto.Entry{{ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}, {ViewStamp:spec.V4o4}, {ViewStamp:spec.V4o5}}, 4},
		{[]proto.Entry{{ViewStamp:spec.V3o3}, {ViewStamp:spec.V4o4}, {ViewStamp:spec.V4o5}},4},
		{[]proto.Entry{{ViewStamp:spec.V4o4}, {ViewStamp:spec.V4o5}}, 4},
		{[]proto.Entry{{ViewStamp:spec.V4o1}, {ViewStamp:spec.V4o2}}, 1},
		{[]proto.Entry{{ViewStamp:spec.V1o2}, {ViewStamp:spec.V4o3}, {ViewStamp:spec.V4o4}}, 2},
		{[]proto.Entry{{ViewStamp:spec.V1o3}, {ViewStamp:spec.V2o4}, {ViewStamp:spec.V4o5}, {ViewStamp:spec.V4o6}},3},
	}
	for i, test := range cases {
		log := New(NewStore())
		log.Append(presetEntries...)
		collisionPos := log.scanCollision(test.entries)
		if collisionPos != test.expCollision {
			t.Errorf("#%d: collision = %d, expected %d", i, collisionPos, test.expCollision)
		}
	}
}

func TestIsUpToDate(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp:spec.V1o1}, {ViewStamp:spec.V2o2}, {ViewStamp:spec.V3o3}}
	log := New(NewStore())
	log.Append(prevEntries...)
	cases := []struct {
		lastOpNum   uint64
		viewNum     uint64
		expUpToDate bool
	}{
		{log.LastOpNum() - 1,4,true},
		{log.LastOpNum(), 4,true},
		{log.LastOpNum() + 1,4,true},
		{log.LastOpNum() - 1,2,false},
		{log.LastOpNum(), 2,false},
		{log.LastOpNum() + 1,2,false},
		{log.LastOpNum() - 1,3,false},
		{log.LastOpNum(),3,true},
		{log.LastOpNum() + 1,3,true},
	}
	for i, test := range cases {
		rv := log.isUpToDate(test.lastOpNum, test.viewNum)
		if rv != test.expUpToDate {
			t.Errorf("#%d: up to date = %v, expected %v", i, rv, test.expUpToDate)
		}
	}
}

func TestUnsafeTryViewNum(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		opNum      uint64
		expOk      bool
		expViewNum uint64
	}{
		{[]proto.Entry{{ViewStamp:spec.V1o5}},5,5,true,1 },
		{[]proto.Entry{{ViewStamp:spec.V1o5}},5,6,false,0 },
		{[]proto.Entry{{ViewStamp:spec.V1o5}},5,4,false,0 },
		{[]proto.Entry{},0,5,false,0 },
	}
	for i, test := range cases {
		u := Unsafe{
			entries: test.entries,
			Offset:  test.offset,
		}
		viewNum, ok := u.tryGetViewNum(test.opNum)
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if viewNum != test.expViewNum {
			t.Errorf("#%d: view-number = %d, expected %d", i, viewNum, test.expViewNum)
		}
	}
}

func TestUnsafeRecover(t *testing.T) {
	initViewStampCase()
	us := Unsafe{
		entries:  []proto.Entry{{ViewStamp:spec.V1o5}},
		Offset:   5,
		appliedState: &proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V1o4}},
	}
	as := proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V2o6}}
	us.recover(as)

	if us.Offset != as.Applied.ViewStamp.OpNum+1 {
		t.Errorf("offset = %d, expected %d", us.Offset, as.Applied.ViewStamp.OpNum+1)
	}
	if len(us.entries) != 0 {
		t.Errorf("len = %d, expected 0", len(us.entries))
	}
	if !reflect.DeepEqual(us.appliedState, &as) {
		t.Errorf("applied state = %v, expected %v", us.appliedState, &as)
	}
}

func TestUnsafeSafeTo(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries   []proto.Entry
		offset    uint64
		opNum     uint64
		viewNum   uint64
		expOffset uint64
		expLen    int
	}{
		{
			[]proto.Entry{}, 0,
			5, 1,
			0, 0,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			5, 1, // safe to the first entry
			6, 0,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V1o6}}, 5,
			5, 1, // safe to the first entry
			6, 1,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V2o6}}, 5,
			6, 1, // safe to the first entry and view-number mismatch
			5, 1,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			4, 1, // safe to old entry
			5, 1,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			4, 2, // safe to old entry
			5, 1,
		},
	}
	for i, test := range cases {
		u := Unsafe{
			entries: test.entries,
			Offset:  test.offset,
		}
		u.safeTo(test.opNum, test.viewNum)
		if u.Offset != test.expOffset {
			t.Errorf("#%d: offset = %d, expected %d", i, u.Offset, test.expOffset)
		}
		if len(u.entries) != test.expLen {
			t.Errorf("#%d: len = %d, expected %d", i, len(u.entries), test.expLen)
		}
	}
}

func TestUnsafeTruncateAndAppend(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		toAppend   []proto.Entry
		expOffset  uint64
		expEntries []proto.Entry
	}{
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			[]proto.Entry{{ViewStamp:spec.V1o6}, {ViewStamp:spec.V1o7}},
			5, []proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V1o6}, {ViewStamp:spec.V1o7}},
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			[]proto.Entry{{ViewStamp:spec.V2o5}, {ViewStamp:spec.V2o6}},
			5, []proto.Entry{{ViewStamp:spec.V2o5}, {ViewStamp:spec.V2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5,
			[]proto.Entry{{ViewStamp:spec.V2o4}, {ViewStamp:spec.V2o5}, {ViewStamp:spec.V2o6}},
			4, []proto.Entry{{ViewStamp:spec.V2o4}, {ViewStamp: spec.V2o5}, {ViewStamp:spec.V2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V1o6}, {ViewStamp:spec.V1o7}}, 5,
			[]proto.Entry{{ViewStamp:spec.V2o6}},
			5, []proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V1o6}, {ViewStamp:spec.V1o7}}, 5,
			[]proto.Entry{{ViewStamp:spec.V2o7}, {ViewStamp:spec.V2o8}},
			5, []proto.Entry{{ViewStamp:spec.V1o5}, {ViewStamp:spec.V1o6}, {ViewStamp:spec.V2o7}, {ViewStamp:spec.V2o8}},
		},
	}
	for i, test := range cases {
		u := Unsafe{
			entries: test.entries,
			Offset:  test.offset,
		}
		u.truncateAndAppend(test.toAppend)
		if u.Offset != test.expOffset {
			t.Errorf("#%d: offset = %d, expected %d", i, u.Offset, test.expOffset)
		}
		if !reflect.DeepEqual(u.entries, test.expEntries) {
			t.Errorf("#%d: entries = %v, expected %v", i, u.entries, test.expEntries)
		}
	}
}

func TestUnsafeTryStartOpNum(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5, nil,
			false, 0,
		},
		{
			[]proto.Entry{}, 0, nil,
			false, 0,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V1o4}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V1o4}},
			true, 5,
		},
	}
	for i, test := range cases {
		us := Unsafe{
			entries:  test.entries,
			Offset:   test.offset,
			appliedState: test.as,
		}
		opNum, ok := us.tryGetStartOpNum()
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if opNum != test.expOpNum {
			t.Errorf("#%d: op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
	}
}

func TestTryLastOpNum(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5, nil,
			true, 5,
		},
		{
			[]proto.Entry{{ViewStamp:spec.V1o5}}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V1o4}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:spec.V1o4}},
			true, 4,
		},
		{
			[]proto.Entry{}, 0, nil,
			false, 0,
		},
	}
	for i, test := range cases {
		us := Unsafe{
			entries: test.entries,
			Offset:  test.offset,
			appliedState: test.as,
		}
		opNum, ok := us.tryGetLastOpNum()
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if opNum != test.expOpNum {
			t.Errorf("#%d: op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
	}
}
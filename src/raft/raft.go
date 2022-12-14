package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	voteFor       int
	curStatus     int //1,2,3==follower, candidate, leader
	electionTimer time.Time
	heartbeat     time.Duration
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	log           Log
	applyCh       chan ApplyMsg
	applyCond     *sync.Cond
}
type Log struct {
	entries []LogEntry
	Index0  int
}
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}
func (log *Log) getLastLog() *LogEntry {
	return &log.entries[len(log.entries)-1]
}
func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.getLastLog().Index; i > 0; i-- {
		term := rf.log.entries[i].Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs Log

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = logs
	}
}
func (rf *Raft) setTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.curStatus = 1
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.log.getLastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		rf.electionTimer = getElectiontime()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func getElectiontime() time.Time {
	return time.Now().Add(time.Duration(150+rand.Int31n(150)) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// select{
		// case <- rf.electionTimer.C:
		// 	rf.mu.Lock()
		// 	rf.election()
		// 	rf.electionTimer.Reset(getElectiontime())
		// 	rf.mu.Unlock()
		// case <-rf.pulseTimer.C:
		// 	rf.mu.Lock()
		// 	if rf.curStatus == 3{
		// 		rf.pulse()
		// 		rf.pulseTimer.Reset(150*time.Millisecond)
		// 	}
		// 	rf.mu.Unlock()

		// }
		time.Sleep(rf.heartbeat)
		rf.mu.Lock()
		if rf.curStatus == 3 {
			rf.Appending(true)
		}
		if time.Now().After(rf.electionTimer) {
			rf.election()
			rf.electionTimer = getElectiontime()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) election() {
	rf.curStatus = 2
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	voted := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log.getLastLog().Index,
				LastLogTerm:  rf.log.getLastLog().Term,
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > args.Term {
					rf.setTerm(reply.Term)
					return
				}
				if reply.Term < args.Term || !reply.VoteGranted {
					return
				}
				if reply.Term == rf.currentTerm {
					if reply.VoteGranted {
						voted++
						if voted > len(rf.peers)/2 {
							rf.curStatus = 3
							for j := range rf.peers {
								rf.nextIndex[j] = rf.log.getLastLog().Index + 1
								rf.matchIndex[j] = 0
							}
							rf.Appending(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.curStatus = 1
						rf.voteFor = -1
						rf.currentTerm = reply.Term
					}
				}
			}

		}(i)
	}
}
func (rf *Raft) Appending(heartbeat bool) {
	for i := range rf.peers {
		nextIndex := rf.nextIndex[i]
		if i == rf.me {
			rf.electionTimer = getElectiontime()
			continue
		}

		if rf.log.getLastLog().Index >= nextIndex || heartbeat {
			if nextIndex > rf.log.getLastLog().Index+1 {
				nextIndex = rf.log.getLastLog().Index
			}
			if nextIndex <= 0 {
				nextIndex = 1
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.log.entries[nextIndex-1].Index,
				PrevLogTerm:  rf.log.entries[nextIndex-1].Term,
				Entries:      make([]LogEntry, rf.log.getLastLog().Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.entries[nextIndex:])
			go func(args AppendEntriesArgs, i int) {
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.setTerm(reply.Term)
					return
				}
				if reply.Success {
					matchIndex := args.PrevLogIndex + len(args.Entries)
					if matchIndex > rf.matchIndex[i] {
						rf.matchIndex[i] = matchIndex
					}
					if matchIndex+1 > rf.nextIndex[i] {
						rf.nextIndex[i] = matchIndex + 1
					}
				} else if reply.Conflict {
					if reply.XTerm != -1 {
						rf.nextIndex[i] = reply.XLen
					} else {
						lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
						if lastLogInXTerm > 0 {
							rf.nextIndex[i] = lastLogInXTerm
						} else {
							rf.nextIndex[i] = reply.XIndex
						}
					}
				} else if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}
				rf.leaderCommit()
			}(args, i)
		}

	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimer = getElectiontime()
	if rf.curStatus == 2 {
		rf.curStatus = 1
	}
	if rf.log.getLastLog().Index < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log.entries)
		reply.Success = false
		reply.Conflict = true
	}
	if rf.log.entries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log.entries[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log.entries[i-1].Term != rf.log.entries[args.PrevLogIndex].Term {
				reply.XIndex = i
				break
			}
			reply.XLen = len(rf.log.entries)
		}
	}
	for idx, entry := range args.Entries {
		if entry.Index <= rf.log.getLastLog().Index && rf.log.entries[entry.Index].Term != entry.Term {
			rf.log.entries = rf.log.entries[:entry.Index]
			rf.persist()
		}
		if entry.Index > rf.log.getLastLog().Index {
			rf.log.entries = append(rf.log.entries, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.log.getLastLog().Index {
			rf.commitIndex = rf.log.getLastLog().Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.apply()
	}
	reply.Success = true

}
func (rf *Raft) leaderCommit() {
	if rf.curStatus != 3 {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.getLastLog().Index; n++ {
		if rf.log.entries[n].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.apply()
				break
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.heartbeat = 150 * time.Millisecond
	rf.electionTimer = getElectiontime()
	rf.curStatus = 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	go rf.applier()
	return rf
}

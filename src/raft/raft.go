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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const LEADER = "Leader"
const FOLLOWER = "Follower"
const CANDIDATE = "Candidate"


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	state string
	currentTerm int
	votedFor int
	time time.Time
	votesReceived int
}

type LogEntry struct {
	// 2A

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	CandidateTerm int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// 2A
	LeaderTerm int
	LeaderId int
}

type AppendEntriesReply struct {
	// 2A
	Term int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	rf.mu.Lock()
	if args.CandidateTerm < rf.currentTerm {
		// reject candidate because it's term is smaller
		// basically don't vote him
		// fmt.Printf("candidate with id=%d rejected because currentTerm > candidateTerm: %d>%d\n", args.CandidateId, rf.currentTerm, args.CandidateTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.CandidateTerm >= rf.currentTerm && rf.votedFor == -1 {
		// if already not voted, vote candidate with 
		// id == args.CandidateId, because it's term is BIGGER
		// fmt.Printf("vote granted to candidate with id=%d because currentTerm < candidateTerm: %d<%d\n", args.CandidateId, rf.currentTerm, args.CandidateTerm)
		rf.currentTerm = args.CandidateTerm
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		rf.time = time.Now()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	rf.mu.Lock()

	if args.LeaderTerm < rf.currentTerm /* - 1 */ {
		// reject and continue in candidate state
		// fmt.Printf("IF1LeaderId=%d, LeaderTerm=%d, myId=%d, currentTerm=%d\n", args.LeaderId, args.LeaderTerm, rf.me, rf.currentTerm)
		reply.Term = args.LeaderTerm
	} else if args.LeaderTerm >= rf.currentTerm /* - 1 */ {
		// accept leader and return to follower state
		// fmt.Printf("IF2LeaderId=%d, LeaderTerm=%d, myId=%d, currentTerm=%d\n", args.LeaderId, args.LeaderTerm, rf.me, rf.currentTerm)
		rf.currentTerm = args.LeaderTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.time = time.Now()
		
		reply.Term = rf.currentTerm
	} 
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//set election timeout between 250 to 450 milliseconds
		elapsedTime := time.Since(rf.time)
		electionTimeoutMs := 300 + (rand.Int63() % 300)
		
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)

		if elapsedTime > time.Duration(electionTimeoutMs) * time.Millisecond && rf.state != LEADER {
			// start elections
			// // rf.mu.Lock()
			// // convert to candidate
			// rf.state = CANDIDATE
			// // increment currentTerm
			// rf.currentTerm++
			// // vote for self
			// rf.votedFor = rf.me
			// // reset election timer
			// rf.time = time.Now()
			// // rf.mu.Unlock()
			// // for i := 0; i < len(rf.peers); i++ {
			// // 	if i == rf.me {
			// // 		continue
			// // 	}
			// // 	request := RequestVoteArgs{}
			// // 	request.CandidateTerm = rf.currentTerm
			// // 	request.CandidateId = rf.me
				
			// // 	reply := RequestVoteReply{}
			// // }
			go rf.startElections()
		}

		rf.mu.Unlock()

		time.Sleep(time.Duration(electionTimeoutMs) * time.Millisecond)
	}
}

func (rf *Raft) startElections() {
	rf.mu.Lock()
	// convert to candidate
	rf.state = CANDIDATE
	// increment currentTerm
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	// reset election timer
	rf.time = time.Now()

	// fmt.Printf("GOT IN ELECTION: id=%d, state=%v, currentTerm=%d\n", rf.me, rf.state, rf.currentTerm)
	totalVoteCount := 0
	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// prepare request struct
		request := RequestVoteArgs{}
		request.CandidateTerm = rf.currentTerm
		request.CandidateId = rf.me

		// prepare reply struct
		reply := RequestVoteReply{}
		
		// send RequestVoteRPC
		go rf.sendRequestVoteWrapper(i, &request, &reply, &totalVoteCount)
	}

	// fmt.Printf("SENT RequestVotes to other peers: id=%d, state=%v, currentTerm=%d\n", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()
	time.Sleep(time.Duration(50) * time.Millisecond)

	rf.mu.Lock()
	// fmt.Printf("IM HERE id=%d\n", rf.me)
	if rf.votesReceived > totalVoteCount / 2 {
		// fmt.Printf("IM NEW LEADER, id=%d, votesReceived=%d, currentTerm=%d\n", rf.me, rf.votesReceived, rf.currentTerm)
		// Candidate got majority of the votes
		rf.state = LEADER
		rf.votedFor = -1
		rf.votesReceived = 0
	} else {
		// Candidate didn't get majority of the votes
		rf.votedFor = -1
		rf.votesReceived = 0
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVoteWrapper(server int, args *RequestVoteArgs, reply *RequestVoteReply, totalVoteCount *int) {
	rf.sendRequestVote(server, args, reply)
	
	rf.mu.Lock()
	if reply.VoteGranted {
		rf.votesReceived++
		*totalVoteCount++
	} else if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.time = time.Now()
		// fmt.Printf("")
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == LEADER {
			// fmt.Printf("Sending heartbeats! leaderId: %d, currentTerm: %d\n", rf.me, rf.currentTerm)
			// send heartbeats
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				request := AppendEntriesArgs{}
				request.LeaderTerm = rf.currentTerm
				request.LeaderId = rf.me

				reply := AppendEntriesReply{}
				
				go rf.sendHeartbeat(i, &request, &reply)
			}
		}

		rf.mu.Unlock()
		// sleep for 150 milliseconds
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	
	// 2A
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.time = time.Now()
	rf.votesReceived = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartbeat goroutine to 
	go rf.sendHeartbeats()

	return rf
}

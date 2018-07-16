package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"time"

	"./errorList"
	"./structs"
)

///////////////////////////////////////////
//			  Global Variables		     //
///////////////////////////////////////////

// Key-value store
var Dictionary map[int](string)

// Map of all stores in the network
var StoreNetwork map[string](structs.Store)

// Server public aaddress
var ServerAddress string

// Leader's address
var LeaderAddress string

// Leader's heartbeat (not used by the leader)
var LeaderHeartbeat time.Time

// Am I leader?
var AmILeader bool

// Am I connected?
var AmIConnected bool

// My public address
var StorePublicAddress string

// My private address
var StorePrivateAddress string

// Logs
var Logs []structs.LogEntry

// CurrentTerm
var CurrentTerm int

// If Store has already voted
var AlreadyVoted bool

///////////////////////////////////////////
//			   Incoming RPC		         //
///////////////////////////////////////////
type Store int

// Consistent Read
// If leader, finds the majority answer from across network and return to client
// If not let client know to re-read from leader
//
// throws 	NonLeaderReadError
//			KeyDoesNotExistError
//			DisconnectedError
func (s *Store) ConsistentRead(key int, value *string) (err error) {
	for LeaderAddress == "" {
	}

	if !AmIConnected {
		return errorList.DisconnectedError(StorePublicAddress)
	}

	if AmILeader {
		if _, exists := Dictionary[key]; exists {
			majorityValue := SearchMajorityValue(key)
			fmt.Printf("Read { Key: %d, Value: %v } \n", key, majorityValue)
			*value = majorityValue
			return nil
			// [?] Do we need to update the network with majorityValue?
		} else {
			return errorList.KeyDoesNotExistError(strconv.Itoa(key))
		}
	}

	return errorList.NonLeaderReadError(LeaderAddress)
}

// Default Read
// If leader respond with value, if not let client know to re-read from leader
//
// throws 	NonLeaderReadError
//			KeyDoesNotExistError
//			DisconnectedError
func (s *Store) DefaultRead(key int, value *string) (err error) {
	for LeaderAddress == "" {
	}

	if !AmIConnected {
		return errorList.DisconnectedError(StorePublicAddress)
	}

	if AmILeader {
		if _, exists := Dictionary[key]; exists {
			fmt.Printf("Read { Key: %d, Value: %v } \n", key, Dictionary[key])
			*value = Dictionary[key]
			return nil
		} else {
			return errorList.KeyDoesNotExistError(strconv.Itoa(key))
		}
	}

	return errorList.NonLeaderReadError(LeaderAddress)
}

// Fast Read
// Returns the value regardless of if it is leader or follower
//
// throws 	KeyDoesNotExistError
//			DisconnectedError
func (s *Store) FastRead(key int, value *string) (err error) {
	if !AmIConnected {
		return errorList.DisconnectedError(StorePublicAddress)
	}
	if _, exists := Dictionary[key]; exists {
		fmt.Printf("Read { Key: %d, Value: %v } \n", key, Dictionary[key])
		*value = Dictionary[key]
		return nil
	}
	return errorList.KeyDoesNotExistError(strconv.Itoa(key))
}

// Write
// Writes a value into key
//
// throws	NonLeaderWriteError
//			DisconnectedError
func (s *Store) Write(request structs.WriteRequest, reply *bool) (err error) {
	for LeaderAddress == "" {
	}

	if !AmIConnected {
		return errorList.DisconnectedError(StorePublicAddress)
	}
	if AmILeader {

		entry := structs.LogEntry{
			Term:        CurrentTerm,
			Index:       len(Logs),
			Key:         request.Key,
			Value:       request.Value,
			IsCommitted: false,
		}

		var prevLog structs.LogEntry
		if len(Logs) != 0 {
			prevLog = Logs[entry.Index-1]
		}

		Log(entry)

		entries := structs.LogEntries{
			Current:  entry,
			Previous: prevLog,
		}

		if len(StoreNetwork) == 0 {

			Dictionary[request.Key] = request.Value
			fmt.Printf("Write { Key: %d, Value: %v } \n", request.Key, request.Value)
			entry.IsCommitted = true
			entry.Index = entry.Index + 1
			Log(entry)
			fmt.Printf("Updated logs after write: %v \n", Logs)

			return nil
		}

		var acks chan *rpc.Call
		var ackUncommitted bool
		var numacks int
		for _, store := range StoreNetwork {
			numacks = 0
			acks = make(chan *rpc.Call, len(StoreNetwork))
			store.RPCClient.Go("Store.WriteLog", entries, &ackUncommitted, acks)
		}

		select {
		case <-acks:
			if ackUncommitted {
				numacks++
			}
		case <-time.After(5 * time.Second):
			fmt.Println("Timed out in WriteLog RPC")
		}

		var acks2 chan *rpc.Call
		var ackCommitted bool
		var numacks2 int
		if numacks >= len(StoreNetwork)/2 {

			prevLog = entry
			entry.IsCommitted = true
			entry.Index = entry.Index + 1

			entries = structs.LogEntries{
				Current:  entry,
				Previous: prevLog,
			}

			Log(entry)
			Dictionary[request.Key] = request.Value
			fmt.Printf("Write { Key: %d, Value: %v } \n", request.Key, request.Value)
			fmt.Printf("Updated logs after write: %v \n", Logs)

			for _, store := range StoreNetwork {
				numacks2 = 0
				acks2 = make(chan *rpc.Call, len(StoreNetwork))
				store.RPCClient.Go("Store.UpdateDictionary", entries, &ackCommitted, acks2)
			}

			select {
			case <-acks2:
				if ackCommitted {
					numacks2++
				}
			case <-time.After(5 * time.Second):
				fmt.Println("Timed out in UpdateDictionary RPC")
			}
		}
	} else {
		return errorList.NonLeaderWriteError(LeaderAddress)
	}
	*reply = true
	return nil
}

func (s *Store) WriteLog(entry structs.LogEntries, ack *bool) (err error) {
	if entry.Current.Term >= CurrentTerm && (len(Logs) == 0 || reflect.DeepEqual(Logs[len(Logs)-1], entry.Previous)) {
		Log(entry.Current)
		*ack = true
	} else {
		*ack = false
	}

	return nil
}

func (s *Store) UpdateDictionary(entry structs.LogEntries, ack *bool) (err error) {
	if entry.Current.Term >= CurrentTerm && (len(Logs) == 0 || reflect.DeepEqual(Logs[len(Logs)-1], entry.Previous)) {
		Log(entry.Current)
		Dictionary[entry.Current.Key] = entry.Current.Value
		fmt.Printf("Updated Dictionary with { Key: [%d], Value: [%v] } \n", entry.Current.Key, entry.Current.Value)
		*ack = true
	} else {
		*ack = false
	}

	return nil
}

// Registers stores with stores
//
func (s *Store) RegisterWithStore(theirInfo structs.StoreInfo, isLeader *bool) (err error) {
	client, _ := rpc.Dial("tcp", theirInfo.Address)
	fmt.Printf("Registering store [%v] completed \n", theirInfo.Address)

	StoreNetwork[theirInfo.Address] = structs.Store{
		Address:   theirInfo.Address,
		RPCClient: client,
		IsLeader:  theirInfo.IsLeader,
	}

	*isLeader = AmILeader
	return nil
}

// UpdateNewStoreLog is when another store requests from a leader to get an updated log.
// Leader will add the requesting store to its StoreNetwork.
func (s *Store) UpdateNewStoreLog(storeAddr string, logEntries *[]structs.LogEntry) (err error) {
	client, _ := rpc.Dial("tcp", storeAddr)

	StoreNetwork[storeAddr] = structs.Store{
		Address:   storeAddr,
		RPCClient: client,
		IsLeader:  false,
	}

	*logEntries = Logs
	return nil
}

// ReceiveHeartbeatFromLeader is a heartbeat signal from the leader to indicate that it is still up.
// If the heartbeat goes over the expected threshhold, there will be a re-electon for a new leader.
// Then, delete the leader from StoreNetwork.
func (s *Store) ReceiveHeartbeatFromLeader(heartbeat structs.Heartbeat, ack *bool) (err error) {
	if !AmIConnected {
		return errorList.DisconnectedError(StorePublicAddress)
	}
	fmt.Println("Heartbeat sent from: ", heartbeat.LeaderAddress)
	CurrentTerm = heartbeat.Term
	LeaderAddress = heartbeat.LeaderAddress
	LeaderHeartbeat = time.Now()
	AmILeader = false
	*ack = true
	AlreadyVoted = false
	return nil
}

// RequestVote is a request for a vote from another store when the re-election is happening.
// It compares the candidate's information with its own and checks whether it is a better candidate.
// Checks in the following order:
// If the number of the candidate's committed logs (DONE writes) is greater than its own, it gives it a vote.
// If the number of the candidate's length of logs is greater than its own, it gives it a vote.
// If after the previous two conditions it is still tied, it gives it a vote.
func (s *Store) RequestVote(candidateInfo structs.CandidateInfo, vote *int) (err error) {

	logLength := len(Logs)
	numberCommittedLogs := ComputeCommittedLogs()

	if candidateInfo.Term >= CurrentTerm && !AlreadyVoted {
		if candidateInfo.NumberOfCommitted >= numberCommittedLogs {
			*vote = 1
			AlreadyVoted = true
		} else if candidateInfo.LogLength >= logLength {
			*vote = 1
			AlreadyVoted = true
		} else {
			*vote = 0
		}
	} else {
		*vote = 0
	}

	return nil
}

// Synchronize current logs to be the same / as up to date as the leader logs
// After synchronized, perform all committed writes to hash table
//
func (s *Store) RollbackAndUpdate(leaderLogs []structs.LogEntry, ack *bool) (err error) {
	leaderIndex := len(leaderLogs) - 1
	currentIndex := len(Logs) - 1
	fmt.Printf("Previous Logs: %v \nPrevious Dictionary: %v \n", Logs, Dictionary)
	if leaderIndex < 0 || currentIndex < 0 {
		return errors.New("Index is negative. Leader log or current log is empty.")
	}

	comparingIndex := 0
	if leaderIndex < currentIndex {
		comparingIndex = leaderIndex
	} else {
		comparingIndex = currentIndex
	}

	for !reflect.DeepEqual(leaderLogs[comparingIndex], Logs[comparingIndex]) {
		comparingIndex = comparingIndex - 1
	}

	SynchronizeLogs(leaderLogs, comparingIndex+1)
	UpdateDictionaryFromLogs()

	fmt.Printf("Updated Logs: %v \nUpdated Dictionary: %v \n", Logs, Dictionary)
	return nil
}

// Called when a store detects a disconnected store. Delete store from map
//
func (s *Store) DeleteDisconnectedStore(address string, ack *bool) (err error) {
	fmt.Println("Store before disconnection update: ", StoreNetwork)
	delete(StoreNetwork, address)
	fmt.Println("Store after disconnection update: ", StoreNetwork)
	*ack = true
	return nil
}

///////////////////////////////////////////
//			   Outgoing RPC		         //
///////////////////////////////////////////

func RegisterWithServer() {
	client, _ := rpc.Dial("tcp", ServerAddress)

	var leaderStore structs.StoreInfo
	var listOfStores []structs.StoreInfo
	var logsToUpdate []structs.LogEntry

	client.Call("Server.RegisterStoreFirstPhase", StorePublicAddress, &leaderStore)

	if leaderStore.Address == StorePublicAddress {

		fmt.Println("Registering with the server successful, you are the leader!")

		LeaderAddress = StorePublicAddress

		AmILeader = true

	} else {

		leaderClient, _ := rpc.Dial("tcp", leaderStore.Address)

		if leaderClient == nil {
			UpdateDisconnectionOnServer(leaderStore.Address)
			LeaderAddress = StorePublicAddress
			AmILeader = true
		} else {
			StoreNetwork[leaderStore.Address] = structs.Store{
				Address:   leaderStore.Address,
				RPCClient: leaderClient,
				IsLeader:  leaderStore.IsLeader,
			}

			leaderClient.Call("Store.UpdateNewStoreLog", StorePublicAddress, &logsToUpdate)

			Logs = logsToUpdate
			UpdateDictionaryFromLogs()
		}

		client.Call("Server.RegisterStoreSecondPhase", StorePublicAddress, &listOfStores)

		fmt.Println("Successfully registered with server. Received store network: ", listOfStores)

		for _, store := range listOfStores {
			if store.IsLeader {
				LeaderAddress = store.Address
			}
			if store.Address != StorePublicAddress && !store.IsLeader {
				RegisterStore(store.Address)
			}
		}

		if leaderClient != nil {
			leaderClient.Close()
		}
	}

	AmIConnected = true
	client.Close()
}

func RegisterStore(store string) {
	var isLeader bool
	client, _ := rpc.Dial("tcp", store)

	myInfo := structs.StoreInfo{
		Address:  StorePublicAddress,
		IsLeader: AmILeader,
	}

	err := client.Call("Store.RegisterWithStore", myInfo, &isLeader)
	if HandleDisconnectedStore(err, store) {
		return
	}

	StoreNetwork[store] = structs.Store{
		Address:   store,
		RPCClient: client,
		IsLeader:  isLeader,
	}

	fmt.Printf("Registered store [%v] into our store network \n", store)
}

func InitHeartbeatLeader() {
	for {
		heartbeat := structs.Heartbeat{Term: CurrentTerm, LeaderAddress: LeaderAddress}
		fmt.Println("Sending heartbeat...")
		for _, store := range StoreNetwork {
			var ack bool
			err := store.RPCClient.Call("Store.ReceiveHeartbeatFromLeader", heartbeat, &ack)
			if HandleDisconnectedStore(err, store.Address) {
				fmt.Printf("Heartbeat was not received, [%v] is disconnected \n", store.Address)
				continue
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func CheckHeartbeat() {
	for {
		if !AmILeader {
			time.Sleep(2 * time.Second)
			currentTime := time.Now()
			if currentTime.Sub(LeaderHeartbeat).Seconds() > 3 {
				fmt.Println("Leader heartbeat was not received on time. Leader election starting...")
				delete(StoreNetwork, LeaderAddress)
				LeaderAddress = ""
				LeaderHeartbeat = time.Time{}
				ElectNewLeader()
			}
		} else {
			break
		}
	}
}

///////////////////////////////////////////
//			  Helper Methods		     //
///////////////////////////////////////////
func SearchMajorityValue(key int) string {
	valueArray := make(map[string]int)
	if len(StoreNetwork) == 0 {
		return Dictionary[key]
	}
	for _, store := range StoreNetwork {
		var value string
		err := store.RPCClient.Call("Store.FastRead", key, &value)
		if HandleDisconnectedStore(err, store.Address) {
			continue
		}

		if value != "" {
			if count, exists := valueArray[value]; exists {
				valueArray[value] = count + 1
			} else {
				valueArray[value] = 1
			}
		}
	}

	tempMaxCount := 0
	majorityValue := ""
	for k, v := range valueArray {
		if v > tempMaxCount {
			v = tempMaxCount
			majorityValue = k
		}
	}

	return majorityValue
}

func Log(entry structs.LogEntry) {
	Logs = append(Logs, entry)
}

func ElectNewLeader() {
	rand.Seed(time.Now().UnixNano())

	numberOfVotes := 1
	candidateInfo := structs.CandidateInfo{
		Term:              CurrentTerm + 1,
		LogLength:         len(Logs),
		NumberOfCommitted: ComputeCommittedLogs(),
	}

	// make himself leader if no stores are in network
	if len(StoreNetwork) == 0 {
		LeaderAddress = StorePublicAddress
		AmILeader = true
		CurrentTerm++
		fmt.Printf("New leader selected: [%v] for term [%d] \n", StorePublicAddress, CurrentTerm)
		go InitHeartbeatLeader()
		UpdateLeadershipOnServer()
	} else {
		var voteReply chan *rpc.Call
		for _, store := range StoreNetwork {
			var vote int
			if LeaderAddress == "" {
				voteReply = make(chan *rpc.Call, 1)
				store.RPCClient.Go("Store.RequestVote", candidateInfo, &vote, voteReply)
			} else {
				break
			}

			select {
			case v := <-voteReply:
				if vote == 1 {

					numberOfVotes = numberOfVotes + vote

					if numberOfVotes > len(StoreNetwork)/2 && LeaderAddress == "" {
						LeaderAddress = StorePublicAddress
						AmILeader = true
						CurrentTerm++
						fmt.Printf("New leader selected: [%v] for term [%d] \n", StorePublicAddress, CurrentTerm)
						go InitHeartbeatLeader()
						RollbackAndUpdate()
						UpdateLeadershipOnServer()
						break
					}
				}
				if HandleDisconnectedStore(v.Error, store.Address) {
					continue
				}
			case <-time.After(time.Duration(rand.Intn(300-150)+150) * time.Millisecond):
				fmt.Println("No clear winner of election. New election starting...")
				CurrentTerm++
				ElectNewLeader()
			}
		}
	}
}

func ComputeCommittedLogs() int {
	numCommittedLogs := 0

	for _, logInfo := range Logs {
		if logInfo.IsCommitted {
			numCommittedLogs++
		}
	}

	return numCommittedLogs
}

func RollbackAndUpdate() {
	for _, store := range StoreNetwork {
		var ack bool
		store.RPCClient.Go("Store.RollbackAndUpdate", Logs, &ack, nil)
		// HandleDisconnectedStore here???
	}
}

func SynchronizeLogs(leaderLogs []structs.LogEntry, syncIndex int) {
	oldLogs := Logs[:syncIndex]
	newLogs := leaderLogs[syncIndex:len(leaderLogs)]
	Logs = append(oldLogs, newLogs...)
}

func UpdateDictionaryFromLogs() {
	newDictionary := make(map[int]string)

	for _, log := range Logs {
		if log.IsCommitted {
			newDictionary[log.Key] = log.Value
		}
	}

	Dictionary = newDictionary
}

func HandleDisconnectedStore(err error, address string) bool {
	isDisconnected := false
	if err != nil {
		if err.Error() == "connection is shut down" {
			isDisconnected = true
			delete(StoreNetwork, address)

			UpdateDisconnectionOnServer(address)

			for _, store := range StoreNetwork {
				var ack bool
				go store.RPCClient.Call("Store.DeleteDisconnectedStore", address, &ack)
			}
		}
	}

	return isDisconnected
}

func UpdateDisconnectionOnServer(address string) {
	var ack bool
	client, _ := rpc.Dial("tcp", ServerAddress)
	err := client.Call("Server.DisconnectStore", address, &ack)
	if err != nil {
		fmt.Println(err)
	}
	client.Close()
}

func UpdateLeadershipOnServer() {
	var ack bool
	client, _ := rpc.Dial("tcp", ServerAddress)
	client.Call("Server.UpdateLeadership", StorePublicAddress, &ack)
	client.Close()
}

// Run store: go run store.go [PublicServerIP:Port] [PublicStoreIP:Port] [PrivateStoreIP:Port]
func main() {
	l := new(Store)
	rpc.Register(l)

	CurrentTerm = 0
	AlreadyVoted = false
	ServerAddress = os.Args[1]
	StorePublicAddress = os.Args[2]
	StorePrivateAddress = os.Args[3]

	Logs = [](structs.LogEntry){}
	Dictionary = make(map[int](string))
	StoreNetwork = make(map[string](structs.Store))

	lis, _ := net.Listen("tcp", StorePrivateAddress)

	go rpc.Accept(lis)

	RegisterWithServer()
	fmt.Println("Leader status: ", AmILeader)

	if AmILeader {
		go InitHeartbeatLeader()
	} else {
		go CheckHeartbeat()
	}

	for {
		conn, _ := lis.Accept()
		go rpc.ServeConn(conn)
	}
}

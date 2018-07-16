package errorList

import "fmt"

// Thrown when client reads a value from non-leader, tells client to request again to leader
// e: leader's address
type NonLeaderWriteError string

func (e NonLeaderWriteError) Error() string {
	return fmt.Sprintf("ERROR: Write value from non-leader store. Please request again to leader address [%s]", string(e))
}

type NonLeaderReadError string

func (e NonLeaderReadError) Error() string {
	return fmt.Sprintf("ERROR: Read value from non-leader store. Please request again to leader address [%s]", string(e))
}

// Thrown when client reads from a key that does not exist
// e: key
type KeyDoesNotExistError string

func (e KeyDoesNotExistError) Error() string {
	return fmt.Sprintf("ERROR: Key [%s] does not exist in database ", string(e))
}

// Thrown when a store is disconnected
// e: address of disconnected entity
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("ERROR: [%s] is disconnected. Please try again.", string(e))
}

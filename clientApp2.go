/*

Represents a Client application that will do READ/WRITE(s) against store nodes.

USAGE:
go run clientApp.go [server ip:port] [client ip:port]

*/

package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"./clientLib"
	"./structs"
)

func main() {
	serverPubIP := os.Args[1]
	clientPubIP := os.Args[2]

	userClient, storeNetwork, _ := clientLib.ConnectToServer(serverPubIP, clientPubIP)
	stores := storeNetwork

	// Write (2, "bonjour")
	errWrite1 := userClient.Write(RandomStoreAddress(stores), 2, "bonjour")
	lAddress1, _ := parseAddressFromError(errWrite1)

	// Retry if not leader
	if lAddress1 != "" {
		errWrite1 = userClient.Write(lAddress1, 2, "bonjour")
	}

	if errWrite1 != nil {
		printError(errWrite1)
	} else {
		printWriteSucess(2, "bonjour")
	}

	time.Sleep(5 * time.Second)

	// Write (1, yeoboseyo)
	errWrite2 := userClient.Write(RandomStoreAddress(stores), 1, "yeoboseyo")
	lAddress2, _ := parseAddressFromError(errWrite2)

	// Retry if not leader
	if lAddress2 != "" {
		errWrite2 = userClient.Write(lAddress2, 1, "yeoboseyo")
	}

	if errWrite2 != nil {
		printError(errWrite2)
	} else {
		printWriteSucess(1, "yeoboseyo")
	}

	// Default (4)
	value1, errRead1 := userClient.DefaultRead(RandomStoreAddress(stores), 4)
	lAddress3, _ := parseAddressFromError(errRead1)

	// Retry if not leader
	if lAddress3 != "" {
		value1, errRead1 = userClient.DefaultRead(lAddress3, 4)
	}

	if value1 != "" {
		printValue(4, value1)
	} else {
		printError(errRead1)
	}

	// Write (3, bonjour)
	errWrite3 := userClient.Write(RandomStoreAddress(stores), 3, "bonjour")
	lAddress4, _ := parseAddressFromError(errWrite3)

	// Retry if not leader
	if lAddress4 != "" {
		errWrite3 = userClient.Write(lAddress4, 3, "bonjour")
	}

	if errWrite3 != nil {
		printError(errWrite3)
	} else {
		printWriteSucess(3, "bonjour")
	}

	// FastRead (5)
	value2, errRead2 := userClient.FastRead(RandomStoreAddress(stores), 7)
	if value2 != "" {
		printValue(7, value2)
	} else {
		printError(errRead2)
	}
}

///////////////////////////////////////////
//	        DUPLICATE FOR EACH APP		 //
///////////////////////////////////////////
//			   Helpers for App			 //
///////////////////////////////////////////
//	        DUPLICATE FOR EACH APP		 //
///////////////////////////////////////////

func HandleError(err error) {
	if err != nil {
		fmt.Println("Error: ", err.Error())
	}
}

// Select a random store address from a list of stores
func RandomStoreAddress(stores []structs.StoreInfo) string {
	randomIndex := random(0, len(stores))
	return stores[randomIndex].Address
}

// returns a random number from a range of [min, max]
func random(min, max int) int {
	source := rand.NewSource(time.Now().UnixNano())
	newRand := rand.New(source)
	return newRand.Intn(max-min) + min
}

func parseAddressFromError(e error) (string, error) {
	if e != nil {
		errorString := e.Error()
		regex := regexp.MustCompile(`\[(.*?)\]`)
		if strings.Contains(errorString, "Read value from non-leader store. Please request again to leader address") || strings.Contains(errorString, "Write value from non-leader store. Please request again to leader address") {
			matchArray := regex.FindStringSubmatch(errorString)

			if len(matchArray) != 0 {
				return matchArray[1], nil
			}
		}
	}

	return "", errors.New("Parsed the wrong error message, does not contain leader address")
}

func printValue(key int, value string) {
	if value != "" {
		fmt.Printf("Read Success { Key: %d, Value: %v } \n", key, value)
	}
}

func printWriteSucess(key int, value string) {
	fmt.Printf("Write Success { Key: %d, Value: %v } \n", key, value)
}
func printError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

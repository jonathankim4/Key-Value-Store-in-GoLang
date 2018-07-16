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

	// Write (3, "hola")
	errWrite1 := userClient.Write(RandomStoreAddress(stores), 3, "hola")
	lAddress1, _ := parseAddressFromError(errWrite1)

	// Retry if not leader
	if lAddress1 != "" {
		errWrite1 = userClient.Write(lAddress1, 3, "hola")
	}

	if errWrite1 != nil {
		printError(errWrite1)
	} else {
		printWriteSucess(3, "hola")
	}

	time.Sleep(5 * time.Second)

	// Default (2)
	value1, errRead1 := userClient.DefaultRead(RandomStoreAddress(stores), 2)
	lAddress2, _ := parseAddressFromError(errRead1)

	// Retry if not leader
	if lAddress2 != "" {
		value1, errRead1 = userClient.DefaultRead(lAddress2, 2)
	}

	if value1 != "" {
		printValue(2, value1)
	} else {
		printError(errRead1)
	}

	// Write (5, guten tag)
	errWrite2 := userClient.Write(RandomStoreAddress(stores), 5, "guten tag")
	lAddress3, _ := parseAddressFromError(errWrite2)

	// Retry if not leader
	if lAddress3 != "" {
		errWrite2 = userClient.Write(lAddress3, 5, "guten tag")
	}

	if errWrite2 != nil {
		printError(errWrite2)
	} else {
		printWriteSucess(5, "guten tag")
	}

	time.Sleep(5 * time.Second)

	// FastRead (5)
	value2, errRead2 := userClient.FastRead(RandomStoreAddress(stores), 1)

	if value2 != "" {
		printValue(1, value2)
	} else {
		printError(errRead2)
	}

	// Default (5)
	value3, errRead3 := userClient.DefaultRead(RandomStoreAddress(stores), 5)
	lAddress4, _ := parseAddressFromError(errRead3)

	// Retry if not leader
	if lAddress4 != "" {
		value3, errRead3 = userClient.DefaultRead(lAddress4, 5)
	}

	if value3 != "" {
		printValue(5, value3)
	} else {
		printError(errRead3)
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

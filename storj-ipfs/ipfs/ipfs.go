// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package ipfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	shell "github.com/ipfs/go-ipfs-api"
)

// DEBUG allows more detailed working to be exposed through the terminal.
var DEBUG = false
var i int = 0

// ConfigIPFS defines the variables and types.
type ConfigIPFS struct {
	HostName  string `json:"hostName"`
	Port      string `json:"port"`
	Path      string `json:"path"`
	ChunkSize string `json:"chunkSize"`
}

// Reader implements an io.Reader interface
type Reader struct {
	Sh         *shell.Shell
	storeBytes []byte
}

// IPFSdata structure for ipfs data
type IPFSdata struct {
	Sh         *shell.Shell
	FilePath   string
	ChunkSize  int64
	FileHandle *os.File
}

// LoadIPFSProperty reads and parses the JSON file.
// that contain a IPFS instance's property.
// and returns all the properties as an object.
func LoadIPFSProperty(fullFileName string) (ConfigIPFS, error) { // fullFileName for fetching IPFS credentials from  given JSON filename.
	var configIPFS ConfigIPFS

	// Open and read the file
	fileHandle, err := os.Open(fullFileName)
	if err != nil {
		return configIPFS, err
	}
	defer fileHandle.Close()

	jsonParser := json.NewDecoder(fileHandle)
	jsonParser.Decode(&configIPFS)

	// Display read information.
	fmt.Println("\nReading IPFS configuration from file: ", fullFileName)
	fmt.Println("Host Name\t: ", configIPFS.HostName)
	fmt.Println("Port\t\t: ", configIPFS.Port)
	fmt.Println("Upload File Path: ", configIPFS.Path)

	return configIPFS, nil
}

// ConnectToIPFSStorj will connect to a IPFS instance,
// based on the read property from an external file.
// It returns a reference to an io.Reader with IPFS instance information.
func ConnectToIPFSStorj(fullFileName string) (*IPFSdata, error) { // fullFileName for fetching  from given JSON filename.

	// Read IPFS instance's properties from an external file.
	configIPFS, err := LoadIPFSProperty(fullFileName)

	if err != nil {
		fmt.Printf("LoadIPFSProperty: %s\n", err)
		return nil, err
	}
	fmt.Println("\nConnecting to IPFS...")

	if configIPFS.HostName == "ipfsHostName" || configIPFS.HostName == "" {
		err1 := errors.New("Invalid HostName")
		return nil, err1
	}
	// Connect IPFS deamon to IPFS node.
	sh := shell.NewShell(configIPFS.HostName + ":" + configIPFS.Port)
	_, _, errVer := sh.Version()
	if errVer != nil {
		err1 := errors.New("Could not find Daemon running")
		return nil, err1
	}

	file, err1 := os.Open(configIPFS.Path)
	if err1 != nil {
		err2 := errors.New("Invalid File path entered")
		return nil, err2
	}

	// Convert size of chunks into int64
	givenSize, _ := strconv.ParseInt(configIPFS.ChunkSize, 10, 64)

	if givenSize <= 0 {
		err1 := errors.New("Invalid chunk size entered")
		return nil, err1
	}

	// Inform about successful connection.
	fmt.Println("Successfully connected to IPFS!")

	// Return IPFS connection object, chunk size and file path.
	return &IPFSdata{Sh: sh, ChunkSize: givenSize, FilePath: configIPFS.Path, FileHandle: file}, nil
}

// CreateCID will connect to a IPFS instance,
// based on the read property from an external file.
// It returns Created CID.
func CreateCID(ipfsData *IPFSdata, data []byte) (string, error) { // fullFileName for fetching  from given JSON filename.

	readers := bytes.NewReader(data)

	// Create encrypt chunk CID
	encryptChunkCID, err := ipfsData.Sh.Add(readers, shell.OnlyHash(true))

	// Return IPFS connection object, chunk size and file path.
	return encryptChunkCID, err
}

// ConnectToIPFSForDownload will connect to a IPFS instance,
// based on the hash name of file on IPFS.
// It returns a reference to an io.Reader with IPFS instance information
func ConnectToIPFSForDownload(hash string, hostName string, port string) (*bytes.Reader, error) { // fullFileName for fetching  from given JSON filename.

	fmt.Println("\nConnecting to IPFS...")
	if hostName == "ipfsHostName" || hostName == "" {
		err1 := errors.New("Invalid HostName")
		return nil, err1
	}

	if hash[0:2] != "Qm" || len(hash) != 46 {
		err1 := errors.New("Invalid Shareable Hash")
		return nil, err1
	}

	// Connect to IPFS daemon to IPFS node.
	sh := shell.NewShell(hostName + ":" + port)
	_, _, errVer := sh.Version()
	if errVer != nil {
		err1 := errors.New("Could not find Daemon running")
		return nil, err1
	}

	// Inform about successful connection.
	fmt.Println("\nSuccessfully connected to IPFS!")

	// Get data from ipfs node.
	fileReader, err := sh.Cat(hash)
	if err != nil {
		fmt.Println("IPFS data read error: ", err)
	}

	// Read all data recive from ipfs.
	readbytes, _ := ioutil.ReadAll(fileReader)
	reader := bytes.NewReader(readbytes)
	return reader, err
}

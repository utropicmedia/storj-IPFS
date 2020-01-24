// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"os"
	"path/filepath"
	ipfs "storj-ipfs/ipfs"
	storj "storj-ipfs/storj"
	"time"

	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"

	shell "github.com/ipfs/go-ipfs-api"
	chunker "github.com/ipfs/go-ipfs-chunker"
	"github.com/urfave/cli"
)

// Configuration files
const ipfsConfigFile = "./config/ipfs_upload.json"
const storjConfigFile = "./config/storj_config.json"
const iPFSDownloadFile = "./config/ipfs_download.json"

var gbDEBUG = false

// Create command-line tool to read from CLI.
var app = cli.NewApp()

// SetAppInfo sets information about the command-line application.
func setAppInfo() {
	app.Name = "Storj IPFS Connector"
	app.Usage = "Backup your IPFS to the decentralized Storj network"
	app.Authors = []*cli.Author{{Name: "UtropicMedia", Email: "development@utropicmedia.com"}}
	app.Version = "1.0.7"

}

// Helper function to flag debug
func setDebug(debugVal bool) {
	gbDEBUG = debugVal
	ipfs.DEBUG = debugVal
	storj.DEBUG = debugVal
}

// setCommands sets various command-line options for the app.
func setCommands() {

	app.Commands = []*cli.Command{
		{
			Name:    "test",
			Aliases: []string{"t"},
			Usage:   "Command to read and parse JSON information about Storj network and upload sample JSON data",
			//\n arguments- 1. fileName [optional] = provide full file name (with complete path), storing Storj configuration information if this fileName is not given, then data is read from ./config/storj_config.json example = ./storj_mongodb s ./config/storj_config.json\n\n\n",
			Action: func(cliContext *cli.Context) error {

				// Default Storj configuration file name.
				var fullFileName = storjConfigFile
				var key string
				var restrict string
				var foundFirstFileName = false
				var foundSecondFileName = false
				// process arguments
				if len(cliContext.Args().Slice()) > 0 {
					for i := 0; i < len(cliContext.Args().Slice()); i++ {

						// Incase, debug is provided as argument.
						if cliContext.Args().Slice()[i] == "debug" {
							setDebug(true)
						} else {
							if !foundFirstFileName {
								fullFileName = cliContext.Args().Slice()[i]
								foundFirstFileName = true
							} else {
								if !foundSecondFileName {
									key = cliContext.Args().Slice()[i]
									foundSecondFileName = true
								} else {
									restrict = cliContext.Args().Slice()[i]
								}
							}
						}
					}
				}
				// Sample database name and data to be uploaded
				fileName := "testdata"
				testData := "test"
				// Converting JSON data to bson data.  TODO: convert to BSON using call to mongo library
				data := []byte(testData)
				if gbDEBUG {
					t := time.Now()
					time := t.Format("2006-01-02")
					fileName = "uploaddata_" + time + ".txt"
					err := ioutil.WriteFile(fileName, data, 0644)
					if err != nil {
						fmt.Println("Error while writting to file ", err)
					}
				}

				var fileNamesDEBUG []string
				var uploadStatus bool
				// Connect to storj network.
				ctx, uplink, project, bucket, storjConfig, _, errr := storj.ConnectStorjReadUploadData(fullFileName, key, restrict)

				// Upload sample data on storj network.
				fileNamesDEBUG, uploadStatus = storj.ConnectUpload(ctx, bucket, data, fileName, fileNamesDEBUG, storjConfig, errr)

				if uploadStatus != true {
					fmt.Println("\nUpload data to IPFS failed.")
					// Close the storj project.
					storj.CloseProject(uplink, project, bucket)
					return errr
				}

				// Close storj project.
				storj.CloseProject(uplink, project, bucket)
				//
				fmt.Println("\nUpload \"testdata\" on Storj: Successful!")
				return errr
			},
		},
		{
			Name:    "store",
			Aliases: []string{"s"},
			Usage:   "Command to connect and transfer ALL files from a desired IPFS instance to given Storj Bucket.",
			//\n    arguments-\n      1. fileName [optional] = provide full file name (with complete path), storing IPFS properties in JSON format\n   if this fileName is not given, then data is read from ./config/ipfs_upload.json\n      2. fileName [optional] = provide full file name (with complete path), storing Storj configuration in JSON format\n     if this fileName is not given, then data is read from ./config/storj_config.json\n   example = ./storj-ipfs store ./config/ipfs_upload.json ./config/storj_config.json\n",
			Action: func(cliContext *cli.Context) error {

				// Default configuration file names.
				var fullFileNameStorj = storjConfigFile
				var fullFileNameIPFS = ipfsConfigFile
				var keyValue string
				var restrict string
				// process arguments - Reading fileName from the command line.
				var foundFirstFileName = false
				var foundSecondFileName = false
				var foundThirdFileName = false
				if len(cliContext.Args().Slice()) > 0 {
					for i := 0; i < len(cliContext.Args().Slice()); i++ {
						// Incase debug is provided as argument.

						if cliContext.Args().Slice()[i] == "debug" {
							setDebug(true)
						} else {
							if !foundFirstFileName {
								fullFileNameIPFS = cliContext.Args().Slice()[i]
								foundFirstFileName = true
							} else {
								if !foundSecondFileName {
									fullFileNameStorj = cliContext.Args().Slice()[i]
									foundSecondFileName = true
								} else {
									if !foundThirdFileName {
										keyValue = cliContext.Args().Slice()[i]
										foundThirdFileName = true
									} else {
										restrict = cliContext.Args().Slice()[i]
									}
								}
							}
						}
					}
				}
				// Connect to storj network and it returns context, uplink, project, bucket and storj configration.
				ctx, uplink, project, bucket, storjConfig, scope, errr := storj.ConnectStorjReadUploadData(fullFileNameStorj, keyValue, restrict)
				if errr != nil {
					fmt.Println(errr)
				}
				var encryptChunkCID string
				var fileNamesDEBUG []string
				var lastFileName string

				// Establish connection with IPFS and get io.Reader implementor.
				ipfsData, err := ipfs.ConnectToIPFSStorj(fullFileNameIPFS)

				if err != nil {
					log.Fatalf("Failed to establish connection with IPFS: %s\n", err)
				}

				if err != nil {
					log.Fatalf("IPFS.FetchData: %s", err)
				} else {
					fmt.Println("Reading ALL content from the IPFS File: Complete!")
				}

				fmt.Println("\nReading content from the file:", ipfsData.FilePath)

				// Get file name from the file path from configration file.
				_, lastFileName = filepath.Split(ipfsData.FilePath)

				// Create encrypt Base CID
				encryptCID, _ := ipfsData.Sh.Add(ipfsData.FileHandle, shell.OnlyHash(true))

				// Close the uploaded file
				ipfsData.FileHandle.Close()

				// Open the uploaded file
				file, err1 := os.Open(ipfsData.FilePath)
				if err1 != nil {
					fmt.Println(err1)
				}

				// Get total size of uploaded file
				statFile, err4 := file.Stat()
				if err4 != nil {
					fmt.Println(err4)
				}
				fileSize := statFile.Size()

				// Generate the number of chunk files
				noOfChunkFiles := int(fileSize/ipfsData.ChunkSize) + 1

				// Divided total uploaded file data into chunks DAG.
				chunkFile := chunker.NewSizeSplitter(file, ipfsData.ChunkSize)

				var metaFile *os.File
				metaFileName := "./metadata.txt"
				os.Remove(metaFileName)
				var uploadStatus bool

				for i := 0; i < noOfChunkFiles; i++ {

					// Get the chunks data from the chunks DAG.
					storeChunkFile, _ := chunkFile.NextBytes()

					//Encrypt the chunk data by the given key
					key := []byte("This is a storj ipfs private key")
					encryptData, err := encrypt(key, storeChunkFile)

					if err != nil {
						log.Fatal(err)
					}

					// Create chunk CID using bytes data
					encryptChunkCID, _ = ipfs.CreateCID(ipfsData, encryptData)

					fileName := encryptCID + "/" + encryptChunkCID

					// Upload chunk data on storj Network with baseCID/chunkCID name.
					fileNamesDEBUG, uploadStatus = storj.ConnectUpload(ctx, bucket, encryptData, fileName, fileNamesDEBUG, storjConfig, errr)

					if uploadStatus != true {
						fmt.Println("Upload data to IPFS failed.")
						// Close the storj project.
						storj.CloseProject(uplink, project, bucket)
						return errr
					}

					// Write all chunks CID into loacl disk file in append mode.
					metaFile, _ = os.OpenFile(metaFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if _, err := metaFile.WriteString(encryptChunkCID + ","); err != nil {
						log.Fatal(err)
					}
					// Close meta file after write all chunks into the file
					metaFile.Close()
				}

				// Open meta file from local disk
				openMetaFile, _ := os.Open(metaFileName)
				metadataBytes, _ := ioutil.ReadAll(openMetaFile)
				// Close Meta file
				openMetaFile.Close()
				// Remove meta file from local disk.
				os.Remove(metaFileName)

				metaFileStoreName := encryptCID + "/" + encryptCID + ".txt"

				// Store meta file data on storj network with baseCID/baseCID.txt
				storj.ConnectUpload(ctx, bucket, metadataBytes, metaFileStoreName, fileNamesDEBUG, storjConfig, errr)
				// Debug the storj data.
				storj.Debug(bucket, metaFileStoreName, storjConfig, lastFileName)

				// Close the storj project.
				storj.CloseProject(uplink, project, bucket)

				fmt.Println("\nAdding configuration data to IPFS: Initiated...")

				// Get the configration data of storj
				configStorj, _ := storj.LoadStorjConfiguration(fullFileNameStorj)

				checkSlash := configStorj.UploadPath[len(configStorj.UploadPath)-1:]
				if checkSlash != "/" {
					configStorj.UploadPath = configStorj.UploadPath + "/"
				}
				ipfsStorjData := configStorj.Bucket + "," + configStorj.UploadPath + "," + lastFileName

				//Encrypt the storj configration data
				enkey := []byte(configStorj.Key)

				ipfsStorjDataBytes := []byte(ipfsStorjData)
				storjEncryptData, err := encrypt(enkey, ipfsStorjDataBytes)
				if err != nil {
					log.Fatal(err)
				}

				var hash []byte
				hash = []byte(encryptCID)

				// Create buffer for Chunk CID and encrypted Storj configurations.
				var encryptedStorjConfig []byte
				encryptedStorjConfig = append(hash, storjEncryptData...)

				// Create the CID from encrypted chunk data and encrypted
				// storj configration and enrypted private key.
				sh := ipfsData.Sh
				configHash, _ := sh.Add(bytes.NewReader(encryptedStorjConfig))

				fmt.Println("Adding configuration data to IPFS: Complete!")
				fmt.Println(" ")
				if keyValue == "key" {
					if restrict == "restrict" {
						fmt.Println("Restricted Serialized Scope Key: ", scope)
						fmt.Println(" ")
					} else {
						fmt.Println("Serialized Scope Key: ", scope)
						fmt.Println(" ")
					}
				}
				fmt.Println("Shareable Hash:", configHash)
				return err
			},
		},
		{
			Name:    "download",
			Aliases: []string{"d"},
			Usage:   "Command to connect and downlaod  ALL files from a desired IPFS instance to given Storj Bucket.",
			//\n arguments- 1. fileName [optional] = provide full file name (with complete path), storing Storj configuration information if this fileName is not given, then data is read from ./config/ipfs_download.json example = ./storj-ipfs d ./config/ipfs_download.json\n\n\n",
			Action: func(cliContext *cli.Context) error {

				// Default Storj configuration file name.
				var downloadedFullFileName = iPFSDownloadFile
				var foundFirstFileName = false
				var keyValue string
				// process arguments
				if len(cliContext.Args().Slice()) > 0 {
					for i := 0; i < len(cliContext.Args().Slice()); i++ {

						// Incase, debug is provided as argument.
						if cliContext.Args().Slice()[i] == "debug" {
							setDebug(true)
						} else {
							if !foundFirstFileName {
								downloadedFullFileName = cliContext.Args().Slice()[i]
								foundFirstFileName = true
							} else {
								keyValue = cliContext.Args().Slice()[i]
							}
						}
					}
				}

				// Read Configration from file
				downloadConfigStorj, err := storj.DownloadStorjConfiguration(downloadedFullFileName)
				if err != nil {
					fmt.Println("loadStorjConfiguration: ", err)
				}

				// Connect and read data from IPFS using file hash and return io.Reader
				reader, err1 := ipfs.ConnectToIPFSForDownload(downloadConfigStorj.FileHash, downloadConfigStorj.HostName, downloadConfigStorj.Port)
				if err1 != nil {
					log.Fatalf("Failed to establish connection with IPFS: %s\n", err1)
				}

				// Download file from storj and save to local disk
				storj.ConnectStorjReadDownloadData(downloadConfigStorj, reader, keyValue)
				return err
			},
		},
	}
}

func main() {

	// Show application information on screen
	setAppInfo()
	// Get command entered by user on cli
	setCommands()
	// Get detailed information for debugging
	setDebug(false)

	err := app.Run(os.Args)

	if err != nil {
		log.Fatalf("app.Run: %s", err)
	}
}

// Encrypt Function to encrypt data with specified key
func encrypt(key, text []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	b := base64.StdEncoding.EncodeToString(text)
	ciphertext := make([]byte, aes.BlockSize+len(b))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	cfb := cipher.NewCFBEncrypter(block, iv)
	cfb.XORKeyStream(ciphertext[aes.BlockSize:], []byte(b))
	return ciphertext, nil
}

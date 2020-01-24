// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package storj

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"storj.io/storj/lib/uplink"
	"storj.io/storj/pkg/macaroon"
)

// DEBUG allows more detailed working to be exposed through the terminal.
var DEBUG = false

// ConfigStorj depicts keys to search for within the stroj_config.json file.
type ConfigStorj struct {
	APIKey               string `json:"apiKey"`
	Satellite            string `json:"satelliteURL"`
	Bucket               string `json:"bucketName"`
	UploadPath           string `json:"uploadPath"`
	EncryptionPassphrase string `json:"encryptionPassphrase"`
	SerializedScope      string `json:"serializedScope"`
	Key                  string `json:"key"`
	DisallowReads        string `json:"disallowReads"`
	DisallowWrites       string `json:"disallowWrites"`
	DisallowDeletes      string `json:"disallowDeletes"`
}

// LoadStorjConfiguration reads and parses the JSON file that contain Storj configuration information.
func LoadStorjConfiguration(fullFileName string) (ConfigStorj, error) { // fullFileName for fetching storj V3 credentials from  given JSON filename.

	var configStorj ConfigStorj

	fileHandle, err := os.Open(fullFileName)
	if err != nil {
		return configStorj, err
	}
	defer fileHandle.Close()

	jsonParser := json.NewDecoder(fileHandle)
	jsonParser.Decode(&configStorj)

	return configStorj, nil
}

// ConnectStorjReadUploadData reads Storj configuration from given file,
// connects to the desired Storj network.
// It then reads data property from an external file.
func ConnectStorjReadUploadData(fullFileName string, keyValue string, restrict string) (context.Context, *uplink.Uplink, *uplink.Project, *uplink.Bucket, ConfigStorj, string, error) { // fullFileName for fetching storj V3 credentials from  given JSON filename
	// databaseReader is an io.Reader implementation that 'reads' desired data,
	// which is to be uploaded to storj V3 network.
	// databaseName for adding dataBase name in storj V3 filename.
	// Read Storj bucket's configuration from an external file.
	var scope string
	configStorj, err := LoadStorjConfiguration(fullFileName)
	if err != nil {
		//return
		fmt.Printf("loadStorjConfiguration: %s", err)
	}

	// Display read information.
	fmt.Println("\nReading Storj configuration from file: ", fullFileName)
	fmt.Println("API Key\t\t\t: ", configStorj.APIKey)
	fmt.Println("Satellite\t	: ", configStorj.Satellite)
	fmt.Println("Bucket	\t	: ", configStorj.Bucket)
	fmt.Println("Upload Path\t\t: ", configStorj.UploadPath)
	fmt.Println("Serialized Scope Key\t: ", configStorj.SerializedScope)

	fmt.Println("\nCreating New Uplink...")

	var cfg uplink.Config
	// Configure the partner id
	cfg.Volatile.PartnerID = "a1ba07a4-e095-4a43-914c-1d56c9ff5afd"

	ctx := context.Background()

	uplinkstorj, err := uplink.NewUplink(ctx, &cfg)
	if err != nil {
		uplinkstorj.Close()
		log.Fatal("Could not create new Uplink object:", err)
	}
	var serializedScope string
	if keyValue == "key" {
		fmt.Println("Parsing the API key...")
		key, err := uplink.ParseAPIKey(configStorj.APIKey)
		if err != nil {
			uplinkstorj.Close()
			log.Fatal("Could not parse API key:", err)
		}

		if DEBUG {
			fmt.Println("API key \t   :", configStorj.APIKey)
			fmt.Println("Serialized API key :", key.Serialize())
		}

		fmt.Println("Opening Project...")
		proj, err := uplinkstorj.OpenProject(ctx, configStorj.Satellite, key)

		if err != nil {
			CloseProject(uplinkstorj, proj, nil)
			log.Fatal("Could not open project:", err)
		}

		// Creating an encryption key from encryption passphrase.
		if DEBUG {
			fmt.Println("\nGetting encryption key from pass phrase...")
		}

		encryptionKey, err := proj.SaltedKeyFromPassphrase(ctx, configStorj.EncryptionPassphrase)
		if err != nil {
			CloseProject(uplinkstorj, proj, nil)
			log.Fatal("Could not create encryption key:", err)
		}

		// Creating an encryption context.
		access := uplink.NewEncryptionAccessWithDefaultKey(*encryptionKey)
		if DEBUG {
			fmt.Println("Encryption access \t:", configStorj.EncryptionPassphrase)
		}
		// Serializing the parsed access, so as to compare with the original key.
		serializedAccess, err := access.Serialize()
		if err != nil {
			CloseProject(uplinkstorj, proj, nil)
			log.Fatal("Error Serialized key : ", err)

		}
		if DEBUG {
			fmt.Println("Serialized access key\t:", serializedAccess)
		}

		// Load the existing encryption access context
		accessParse, err := uplink.ParseEncryptionAccess(serializedAccess)
		if err != nil {
			log.Fatal(err)
		}

		if restrict == "restrict" {
			disallowRead, _ := strconv.ParseBool(configStorj.DisallowReads)
			disallowWrite, _ := strconv.ParseBool(configStorj.DisallowWrites)
			disallowDelete, _ := strconv.ParseBool(configStorj.DisallowDeletes)
			userAPIKey, err := key.Restrict(macaroon.Caveat{
				DisallowReads:   disallowRead,
				DisallowWrites:  disallowWrite,
				DisallowDeletes: disallowDelete,
			})
			if err != nil {
				log.Fatal(err)
			}

			userAPIKey, userAccess, err := accessParse.Restrict(userAPIKey,
				uplink.EncryptionRestriction{
					Bucket:     configStorj.Bucket,
					PathPrefix: configStorj.UploadPath,
				},
			)
			if err != nil {
				log.Fatal(err)
			}
			userRestrictScope := &uplink.Scope{
				SatelliteAddr:    configStorj.Satellite,
				APIKey:           userAPIKey,
				EncryptionAccess: userAccess,
			}
			serializedRestrictScope, err := userRestrictScope.Serialize()
			if err != nil {
				log.Fatal(err)
			}
			scope = serializedRestrictScope
			//fmt.Println("Restricted serialized user scope", serializedRestrictScope)
		}

		userScope := &uplink.Scope{
			SatelliteAddr:    configStorj.Satellite,
			APIKey:           key,
			EncryptionAccess: access,
		}
		serializedScope, err = userScope.Serialize()
		if err != nil {
			log.Fatal(err)
		}
		if restrict == "" {
			scope = serializedScope
			//fmt.Println("serialized user scope", serializedScope)
		}

		proj.Close()
		uplinkstorj.Close()
	} else {
		serializedScope = configStorj.SerializedScope

	}
	parsedScope, err := uplink.ParseScope(serializedScope)
	if err != nil {
		log.Fatal(err)
	}

	uplinkstorj, err = uplink.NewUplink(ctx, &cfg)
	if err != nil {
		log.Fatal("Could not create new Uplink object:", err)
	}
	proj, err := uplinkstorj.OpenProject(ctx, parsedScope.SatelliteAddr, parsedScope.APIKey)
	if err != nil {
		CloseProject(uplinkstorj, proj, nil)
		log.Fatal("Could not open project:", err)
	}

	fmt.Println("Opening Bucket\t: ", configStorj.Bucket)
	// Open up the desired Bucket within the Project.
	bucket, err := proj.OpenBucket(ctx, configStorj.Bucket, parsedScope.EncryptionAccess)
	if err != nil {
		fmt.Println("Could not open bucket", configStorj.Bucket, ":", err)
		fmt.Println("Trying to create new bucket....")
		_, err1 := proj.CreateBucket(ctx, configStorj.Bucket, nil)
		if err1 != nil {
			CloseProject(uplinkstorj, proj, bucket)
			fmt.Printf("Could not create bucket %q:", configStorj.Bucket)
			log.Fatal(err1)
		} else {
			fmt.Println("Created Bucket", configStorj.Bucket)
		}
		fmt.Println("Opening created Bucket: ", configStorj.Bucket)
		bucket, err = proj.OpenBucket(ctx, configStorj.Bucket, parsedScope.EncryptionAccess)
		if err != nil {
			fmt.Printf("Could not open bucket %q: %s", configStorj.Bucket, err)
		}
	}
	return ctx, uplinkstorj, proj, bucket, configStorj, scope, err
}

// ConnectUpload uploads the data to storj network.
func ConnectUpload(ctx context.Context, bucket *uplink.Bucket, data []byte, databaseName string, fileNamesDEBUG []string, configStorj ConfigStorj, err error) ([]string, bool) {
	// Read data using bytes and upload it to Storj.
	var file []string
	file = fileNamesDEBUG
	var uploadComplete = false
	for err = io.ErrShortBuffer; err == io.ErrShortBuffer; {

		var filename = databaseName
		var retryCount = 0
		checkSlash := configStorj.UploadPath[len(configStorj.UploadPath)-1:]
		if checkSlash != "/" {
			configStorj.UploadPath = configStorj.UploadPath + "/"
		}
		fmt.Println("\nUpload Object Path: ", configStorj.UploadPath+filename)
		fmt.Printf("Upload %d bytes of object to Storj bucket: Initiated...\n", len(data))

		for retryCount < 5 {
			readerBytes := bytes.NewReader(data)
			readerIO := io.Reader(readerBytes)
			err = bucket.UploadObject(ctx, configStorj.UploadPath+filename, readerIO, nil)
			if err != nil {
				retryCount++
				fmt.Println("Retrying...")
			} else {
				uploadComplete = true
				break
			}
		}

		if DEBUG {
			file = append(file, filename)
		}
	}

	if err != nil {
		fmt.Printf("Could not upload: %s", err)
		return nil, uploadComplete
	}

	fmt.Println("Uploading object to Storj bucket: Completed!")
	return file, uploadComplete
}

// Debug function downloads the data from storj bucket after upload to verify data is uploaded successfully.
func Debug(bucket *uplink.Bucket, metaFileName string, configStorj ConfigStorj, lastFileName string) {

	if DEBUG {
		ctx := context.Background()
		splitCid := strings.Split(string(metaFileName), "/")
		baseCID := splitCid[0]
		checkSlash := configStorj.UploadPath[len(configStorj.UploadPath)-1:]
		if checkSlash != "/" {
			configStorj.UploadPath = configStorj.UploadPath + "/"
		}
		//Get Meta data file from storj
		readBackMeta, err := bucket.OpenObject(ctx, configStorj.UploadPath+metaFileName)
		if err != nil {
			fmt.Printf("Could not open object at %s: ", configStorj.UploadPath+metaFileName)
			log.Fatal(err)

		}
		defer readBackMeta.Close()

		// We want the whole thing, so r a nge from 0 to -1.
		strmMeta, err := readBackMeta.DownloadRange(ctx, 0, -1)
		if err != nil {
			log.Fatal("Could not initiate download:", err)
		}
		defer strmMeta.Close()

		// Read everything from the stream.
		receivedContentsMeta, err := ioutil.ReadAll(strmMeta)

		//Convert byte array into String
		receiveContentsMeta := string(receivedContentsMeta)
		receiveContentsMeta = strings.TrimSuffix(receiveContentsMeta, ",")

		downloadFileNamesDEBUG := strings.Split(receiveContentsMeta, ",")

		os.Remove("debug/" + lastFileName)

		var downloadFileDisk *os.File
		for _, filename := range downloadFileNamesDEBUG {
			// Test uploaded data by downloading it.
			// serializedAccess, err := access.Serialize().
			// Initiate a download of the same object again.
			readBack, err := bucket.OpenObject(ctx, configStorj.UploadPath+baseCID+"/"+filename)
			if err != nil {
				fmt.Printf("Could not open object at %q: %v", configStorj.UploadPath+baseCID+"/"+filename, err)
				log.Fatal(err)
			}
			defer readBack.Close()

			fmt.Println("\nDownloading file uploaded on storj...")
			// We want the whole thing, so range from 0 to -1.
			strm, err := readBack.DownloadRange(ctx, 0, -1)
			if err != nil {
				fmt.Printf("Could not initiate download: %v", err)
			}
			defer strm.Close()
			fmt.Printf("Downloading Object %s from bucket : Initiated...\n", filename)
			// Read everything from the stream.
			receivedContents, err := ioutil.ReadAll(strm)
			if err != nil {
				fmt.Printf("Could not read object: %v", err)
			}

			//Decrypt the storj data
			hmkey := []byte("This is a storj ipfs private key")
			dec, _ := decrypt(hmkey, receivedContents)

			downloadFileDisk, _ = os.OpenFile("debug/"+lastFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			_, err = downloadFileDisk.Write(dec)
			if err != nil {
				log.Fatal(err)
			}
			defer downloadFileDisk.Close()
			readBack.Close()
			strm.Close()
			downloadFileDisk.Close()
			fmt.Printf("Downloaded %d bytes of Object from bucket!\n", len(receivedContents))

		}
		fmt.Printf("File downloading: Complete!\n")
		fmt.Printf("\nDebug file \"%s\" downloaded to \"%s\"\n", lastFileName, "debug/")
	}

}

// CloseProject closes bucket, project and uplink.
func CloseProject(uplink *uplink.Uplink, proj *uplink.Project, bucket *uplink.Bucket) {
	if bucket != nil {
		bucket.Close()
	}

	if proj != nil {
		proj.Close()
	}

	if uplink != nil {
		uplink.Close()
	}
}

// DownloadConfigStorj structure to store data from json file
type DownloadConfigStorj struct {
	HostName             string `json:"hostName"`
	Port                 string `json:"port"`
	FileHash             string `json:"shareableHash"`
	DownloadPath         string `json:"downloadPath"`
	APIKey               string `json:"apiKey"`
	SatelliteURL         string `json:"satelliteURL"`
	EncryptionPassphrase string `json:"encryptionPassphrase"`
	SerializedScope      string `json:"serializedScope"`
	Key                  string `json:"key"`
}

// DownloadStorjConfiguration reads and parses the JSON file that contain Storj configuration information.
func DownloadStorjConfiguration(fullFileName string) (DownloadConfigStorj, error) { // fullFileName for fetching storj V3 credentials from  given JSON filename.

	var downloadConfigStorj DownloadConfigStorj

	fileHandle, err := os.Open(fullFileName)
	if err != nil {
		return downloadConfigStorj, err
	}
	defer fileHandle.Close()

	jsonParser := json.NewDecoder(fileHandle)
	jsonParser.Decode(&downloadConfigStorj)

	// Display read information.
	fmt.Println("\nReading Download configuration from file: ", fullFileName)
	fmt.Println("Host Name\t\t: ", downloadConfigStorj.HostName)
	fmt.Println("Port\t\t\t: ", downloadConfigStorj.Port)
	fmt.Println("Download Path\t\t: ", downloadConfigStorj.DownloadPath)
	fmt.Println("Serialized Scope Key\t: ", downloadConfigStorj.SerializedScope)

	return downloadConfigStorj, nil
}

// ConnectStorjReadDownloadData function downloads data from Storj
func ConnectStorjReadDownloadData(downloadConfigStorj DownloadConfigStorj, readFile *bytes.Reader, keyValue string) error {
	var downloadAPIKey string
	var downloadSatellite string
	var downloadBucket string
	var downloadPath string
	var downloadFileName string
	var serializedScope string
	makebuffer := make([]byte, 46)

	// Read data from IPFS
	readHashData, err := readFile.Read(makebuffer)
	if err != nil {
		fmt.Println("Error reading file:", err)
	}

	// Seperate the Hash and configration data
	data := []byte(makebuffer[0:readHashData])
	downloadFileName = string(data)
	downloadFileSize := readFile.Size()

	restDataBuf := make([]byte, downloadFileSize-46)
	readRestFile, err := readFile.Read(restDataBuf)
	if err != nil {
		fmt.Println("Error reading file:", err)
	}

	dataEnc := []byte(restDataBuf[0:readRestFile])
	pkey := []byte(downloadConfigStorj.Key)

	// Decrypt the configration data
	decryptData, err := decrypt(pkey, dataEnc)
	if err != nil {
		log.Fatal(err)
	}

	// Create directory if not present
	if _, err := os.Stat(downloadConfigStorj.DownloadPath); os.IsNotExist(err) {
		err1 := os.Mkdir(downloadConfigStorj.DownloadPath, os.ModeDir)
		if err1 != nil {
			log.Fatal("Invalid Download Path")
		}
	}

	// Split the configration data
	splitStorjData := strings.Split(string(decryptData), ",")
	downloadAPIKey = downloadConfigStorj.APIKey          //splitStorjData[0]
	downloadSatellite = downloadConfigStorj.SatelliteURL //splitStorjData[1]
	downloadBucket = splitStorjData[0]
	downloadPath = splitStorjData[1]
	lastFileName := splitStorjData[2]

	// Configure the partner id
	var cfg uplink.Config
	cfg.Volatile.PartnerID = "a1ba07a4-e095-4a43-914c-1d56c9ff5afd"
	ctx := context.Background()

	uplinkstorj, err := uplink.NewUplink(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("Could not create new Uplink object: %s", err)
	}
	defer uplinkstorj.Close()

	if keyValue == "key" {
		fmt.Println("Parsing the API key...")
		key, err := uplink.ParseAPIKey(downloadAPIKey)
		if err != nil {
			return fmt.Errorf("Could not parse API key: %s", err)
		}

		if DEBUG {
			fmt.Println("API key \t   :", downloadAPIKey)
			fmt.Println("Serialized API key :", key.Serialize())
		}

		// Open the project
		fmt.Println("Opening Project...")
		proj, err := uplinkstorj.OpenProject(ctx, downloadSatellite, key)

		if err != nil {
			return fmt.Errorf("Could not open project: %s", err)
		}
		defer proj.Close()

		// Creating an encryption key from encryption passphrase.
		if DEBUG {
			fmt.Println("\nGetting encryption key from pass phrase...")
		}

		encryptionKey, err := proj.SaltedKeyFromPassphrase(ctx, downloadConfigStorj.EncryptionPassphrase)
		if err != nil {
			return fmt.Errorf("Could not create encryption key: %s", err)
		}

		// Creating an encryption context.
		access := uplink.NewEncryptionAccessWithDefaultKey(*encryptionKey)
		if DEBUG {
			fmt.Println("Encryption access \t:", downloadConfigStorj.EncryptionPassphrase)
		}

		// Serializing the parsed access, so as to compare with the original key.
		serializedAccess, err := access.Serialize()
		if err != nil {
			fmt.Println("Error Serialized key : ", err)
		}

		if DEBUG {
			fmt.Println("Serialized access key\t:", serializedAccess)
		}

		userScope := &uplink.Scope{
			SatelliteAddr:    downloadConfigStorj.SatelliteURL,
			APIKey:           key,
			EncryptionAccess: access,
		}
		serializedScope, err = userScope.Serialize()
		if err != nil {
			log.Fatal(err)
		}

		proj.Close()
		uplinkstorj.Close()
	} else {
		serializedScope = downloadConfigStorj.SerializedScope

	}
	parsedScope, err := uplink.ParseScope(serializedScope)
	if err != nil {
		log.Fatal(err)
	}

	uplinkstorj, err = uplink.NewUplink(ctx, &cfg)
	if err != nil {
		log.Fatal("Could not create new Uplink object:", err)
	}
	proj, err := uplinkstorj.OpenProject(ctx, parsedScope.SatelliteAddr, parsedScope.APIKey)
	if err != nil {
		CloseProject(uplinkstorj, proj, nil)
		log.Fatal("Could not open project:", err)
	}
	fmt.Println("Opening Bucket: ", downloadBucket)

	// Open up the desired Bucket within the Project.
	bucket, err := proj.OpenBucket(ctx, downloadBucket, parsedScope.EncryptionAccess)
	if err != nil {
		return fmt.Errorf("Could not open bucket %q: %s", downloadBucket, err)
	}

	// Download meta file from storj network.
	metaFileName := downloadFileName + "/" + downloadFileName + ".txt"
	//Get Meta data file from storj
	readBackMeta, err := bucket.OpenObject(ctx, downloadPath+metaFileName)
	if readBackMeta == nil {
		log.Fatal("Could not read object: Access Denied")
	}
	if err != nil {
		return fmt.Errorf("could not open object at %q: %v", downloadPath+metaFileName, err)
	}
	bucket.Close()
	// We want the whole thing, so r a nge from 0 to -1.
	strmMeta, err := readBackMeta.DownloadRange(ctx, 0, -1)
	if err != nil {
		return fmt.Errorf("Could not initiate download: %v", err)
	}
	readBackMeta.Close()

	// Read everything from the stream.
	receivedContentsMeta, err := ioutil.ReadAll(strmMeta)

	strmMeta.Close()
	//Convert byte array into String
	receiveContentsMeta := string(receivedContentsMeta)

	receiveContentsMeta = strings.TrimSuffix(receiveContentsMeta, ",")

	downloadFileNamesDEBUG := strings.Split(receiveContentsMeta, ",")

	var fileNameDownload = downloadConfigStorj.DownloadPath + "/" + lastFileName

	os.Remove(fileNameDownload)
	hmkey := []byte("This is a storj ipfs private key")

	var downloadFileDisk *os.File
	for _, filename := range downloadFileNamesDEBUG {
		readBack, err := bucket.OpenObject(ctx, downloadPath+downloadFileName+"/"+filename)
		if err != nil {
			return fmt.Errorf("Could not open object at %q: %v", downloadPath+downloadFileName+"/"+filename, err)
		}

		fmt.Println("\nInitiating download...")
		// We want the whole thing, so range from 0 to -1.
		strm, err := readBack.DownloadRange(ctx, 0, -1)
		if err != nil {
			return fmt.Errorf("Could not initiate download: %v", err)
		}
		readBack.Close()
		fmt.Printf("Downloading Object %s from bucket : Initiated...\n", filename)

		// Read everything from the stream.
		receivedContents, err := ioutil.ReadAll(strm)
		if err != nil {
			return fmt.Errorf("Could not Read All content in stream: %v", err)
		}
		strm.Close()

		//Decryt the downloaded file data from storj

		dec, err := decrypt(hmkey, receivedContents)
		if err != nil {
			return fmt.Errorf("Could not decrypt received data: %v", err)
		}

		// Store the downloaded file from storj in local disk

		downloadFileDisk, err = os.OpenFile(fileNameDownload, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("Could not open file to write downloaded data: %v", err)
		}

		if _, err := downloadFileDisk.Write(dec); err != nil {
			log.Fatal(err)
		}
		downloadFileDisk.Sync()

		downloadFileDisk.Close()

		fmt.Printf("Downloaded %d bytes of Object from bucket!\n", len(receivedContents))

	}
	fmt.Printf("File downloading: Complete!\n")
	fmt.Printf("\nFile \"%s\" downloaded to \"%s\"\n", lastFileName, downloadConfigStorj.DownloadPath)
	return nil
}

// Function to decrypt data based on given key.
func decrypt(key, text []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(text) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}
	iv := text[:aes.BlockSize]
	text = text[aes.BlockSize:]
	cfb := cipher.NewCFBDecrypter(block, iv)
	cfb.XORKeyStream(text, text)
	data, err := base64.StdEncoding.DecodeString(string(text))
	if err != nil {
		return nil, err
	}
	iv = nil
	text = nil
	cfb = nil
	return data, nil
}

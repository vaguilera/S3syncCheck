package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/logrusorgru/aurora"
	"github.com/spf13/viper"
)

type File struct {
	name  string
	isDir bool
	md5   string
}

func md5file(file string) string {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return hex.EncodeToString(h.Sum(nil))
}

func localfiles(dataFolder string) ([]File, error) {
	var files []File
	var currentMD5 string

	root := "./" + dataFolder

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if path == root {
			return nil
		}

		if info.IsDir() {
			currentMD5 = ""
		} else {
			currentMD5 = md5file(path)
		}
		files = append(files, File{
			name:  strings.Replace(strings.TrimPrefix(path, dataFolder+"\\"), "\\", "/", -1),
			isDir: info.IsDir(),
			md5:   currentMD5,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func remotefiles(region string, bucketName string) ([]File, error) {
	var files []File

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	fmt.Printf("Checking s3 Bucket objects (%s)...\n\n", bucketName)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		return nil, err
	}

	var isfolder bool
	for _, item := range resp.Contents {
		isfolder = false
		if (*item.Key)[len(*item.Key)-1:] == "/" {
			isfolder = true
		}
		files = append(files, File{
			name:  *item.Key,
			isDir: isfolder,
			md5:   (*item.ETag)[1 : len(*item.ETag)-1],
		})
	}
	return files, nil
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	localFiles, err := localfiles(viper.GetString("localfolder"))
	if err != nil {
		panic(fmt.Errorf("Error retrieving localfiles from: %s", viper.GetString("localfolder")))
	}
	if len(localFiles) == 0 {
		panic(fmt.Errorf("No files found in: %s", viper.GetString("localfolder")))
	}

	s3Files, err := remotefiles(viper.GetString("region"), viper.GetString("bucketname"))
	if err != nil {
		panic(fmt.Errorf("Error retrieving files from S3: %s", err))
	}

	var found bool
	fmt.Println("----- S3 Files -----")
	for i := range s3Files {
		if s3Files[i].isDir {
			continue
		}
		found = false
		for j := range localFiles {
			if s3Files[i].name == localFiles[j].name {

				if s3Files[i].md5 != localFiles[j].md5 {
					fmt.Printf("%s - Checksum error - S3[%s] - Local[%s]\n", s3Files[i].name, Green(s3Files[i].md5), Red(localFiles[j].md5))
				}
				found = true
				localFiles = append(localFiles[:j], localFiles[j+1:]...)
				break
			}
		}
		if !found {
			fmt.Println(s3Files[i].name, "Not found")
		}
	}
	fmt.Println("\n----- Local Files -----")
	for i := range localFiles {
		if localFiles[i].isDir {
			continue
		}
		fmt.Println(localFiles[i].name, "Not found")
	}
}

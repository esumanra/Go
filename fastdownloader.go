/*
Demonstration of multipart download of file.

Instead of downloading a big file at once, it can be concurrently downloade in chunks.
Real world application are - Internet Download Manager (IDM), Ninja Downloader
A file which has capability of downloading in multi-part can be determined by header - Accept-Ranges:[bytes]
We can set this header to download smaller chunk of file by splitting into n-chunks and download concurrently
*/

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type downloadInfo struct {
	url          string
	downloadPath string
	sections     int
}

func getSegments(size int, noOfSegments int) [][2]int {
	segments := make([][2]int, noOfSegments)
	segmentSize := size / noOfSegments
	for i := 0; i < noOfSegments; i++ {
		if i == 0 {
			segments[i][0] = 0
		} else {
			segments[i][0] = segments[i-1][1] + 1
		}
		if i < noOfSegments-1 {
			segments[i][1] = segments[i][0] + segmentSize
		} else {
			segments[i][1] = size - 1
		}
	}
	return segments
}

func download(info downloadInfo, async bool) {
	// requesting for headers
	res, err := http.Head(info.url)
	if err != nil {
		fmt.Println("URL is incorrect:", info.url)
	}
	fmt.Println(res)
	fileSize, _ := strconv.Atoi(res.Header.Values("Content-Length")[0])
	fmt.Println("file size is:", fileSize)
	segments := getSegments(fileSize, 10)
	_, isMultipartDownloadSupported := res.Header["Accept-Ranges"]

	if !isMultipartDownloadSupported {
		fmt.Println("Falling to sequential download. URL does not support multi part download")
	}
	if async && isMultipartDownloadSupported {
		var wg sync.WaitGroup
		for no, segment := range segments {
			wg.Add(1)
			go func(segment [2]int, no int) {
				defer wg.Done()
				downloadSegment(info, segment, no)
			}(segment, no)
		}
		wg.Wait()
		mergeSegments(info, segments)
	} else {
		resp, _ := http.Get(info.url)
		body, _ := ioutil.ReadAll(resp.Body)
		ioutil.WriteFile(info.downloadPath, body, os.ModePerm)
	}

}

func downloadSegment(info downloadInfo, segment [2]int, no int) error {
	c := http.Client{}
	req, _ := http.NewRequest("GET", info.url, nil)
	bytes := fmt.Sprintf("bytes=%v-%v", segment[0], segment[1])
	req.Header.Add("Range", bytes)
	resp, err := c.Do(req)
	if err != nil {
		fmt.Printf("error %s", err)
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	ioutil.WriteFile(fmt.Sprintf("section-%v.tmp", no), body, os.ModePerm)

	return nil
}

func mergeSegments(info downloadInfo, segments [][2]int) error {
	os.Remove(info.downloadPath)
	f, err := os.OpenFile(info.downloadPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	for i := range segments {
		tmpFileName := fmt.Sprintf("section-%v.tmp", i)
		b, err := ioutil.ReadFile(tmpFileName)
		if err != nil {
			return err
		}
		n, err := f.Write(b)
		if err != nil {
			return err
		}
		err = os.Remove(tmpFileName)
		if err != nil {
			return err
		}
		fmt.Printf("%v bytes merged\n", n)
	}
	return nil
}

func main() {
	startTime := time.Now()

	info := downloadInfo{
		// test with file that are big in size atleast ~500MB or big video links if you have FAST INTERNET to notice difference
		url:          "https://golang.org/dl/go1.17.2.windows-386.msi",
		downloadPath: "go1.17.2.windows-386.msi",
		sections:     8,
	}

	// Sequential donwload
	// download(info, false)

	// Concurrent download
	download(info, true)
	fmt.Printf("Download took %v seconds\n", time.Since(startTime).Seconds())
}

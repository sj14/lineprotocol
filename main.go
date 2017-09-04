package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	inputFlag      string
	outdirFlag     string
	replayFlag     bool
	importFileFlag bool
	dbFlag         string
	tableFlag      string
	rpFlag         string
	fakeTimeFlag   bool
)

func init() {
	// Flags
	flag.StringVar(&inputFlag, "input", "path/to/file.csv", "CSV file or directory to convert")
	flag.StringVar(&outdirFlag, "output", "converted/", "output directory")
	flag.BoolVar(&replayFlag, "replay", false, "create a replay file (.srpl)")
	flag.BoolVar(&importFileFlag, "importFile", false, "create an influx import file (.txt)")
	flag.StringVar(&dbFlag, "db", "mydb", "database name (only for replay file)")
	flag.StringVar(&tableFlag, "table", "table_test", "table name")
	flag.StringVar(&rpFlag, "rp", "autogen", "database retention policy (only for replay file)")
	flag.BoolVar(&fakeTimeFlag, "time", false, "Add a fake timestamp to the data")
	flag.Parse()
}

func main() {
	if isDir(inputFlag) {
		err := filepath.Walk(inputFlag, convertDir)
		if err != nil {
			log.Println("Not able to convert file: ", err)
		}
	} else {
		convertFile(inputFlag)
	}
}

func convertDir(path string, f os.FileInfo, err error) error {
	convertFile(path)
	return nil
}

func convertFile(path string) {
	var lpStr string

	// Get filetype by file extension
	ftype := filepath.Ext(path)
	// remove leading dot in file extension
	ftype = strings.Replace(ftype, ".", "", -1)

	// If filetype is not csv, stop execution
	if ftype != "csv" {
		//log.Println("Only CSV files allowed. Input Filetype: ", ftype)
		return
	}

	// Set default output filename to input filename without extension
	outfile := strings.TrimSuffix(path, filepath.Ext(path))

	// Set output file extension accordingly
	if replayFlag {
		outfile = outfile + ".srpl"
	} else {
		outfile = outfile + ".txt"
	}

	// Do conversion
	lpStr = csvToStreamReplay(path)

	// Replay files are compressed
	if replayFlag {
		lpStr = gzipString(lpStr)
	}

	writeFile(lpStr, outfile)
}

func csvToStreamReplay(path string) string {
	// Open CSV file
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("Could not open CSV file: ", err)
	}

	// Read CSV file
	r := csv.NewReader(bufio.NewReader(f))
	result, err := r.ReadAll()
	if err != nil {
		log.Println("Could not read CSV file: ", err)
	}

	var lpStr string
	// timestamp starts at 1970-01-01 00:00:00
	var ftimestamp uint64

	// Should an influx import file be created?
	// Add database at the top of the file
	if importFileFlag {
		lpStr = fmt.Sprintf(
			"# DDL\n"+
				"CREATE DATABASE %v\n\n"+
				"# DML\n"+
				"# CONTEXT-DATABASE: %v\n\n",
			dbFlag, dbFlag)
	}

	// Range over each line in the CSV file
	for i := range result {
		// First column is the id or time
		idStr := result[i][0]

		// Second column is the measured value
		valueStr := result[i][1]
		valueFloat, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			log.Println("Could not convert CSV value to float: ", err)
		}

		// Create a proper formatted line protocal string
		lpStr = lpStr + lineProtocolFormat(tableFlag, valueFloat, idStr, ftimestamp)

		// Fake next used timestamp by incrementing 10 seconds
		ftimestamp = ftimestamp + 10000000000
	}
	return lpStr
}

func lineProtocolFormat(tableName string, value float64, key string, ftimestamp uint64) string {
	// Convert to line protocol
	//
	// Line Protocol Syntax:
	// <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
	// Example: cpu_load_short,host=server02,region=us-west value=0.55 1422568543702900257
	var lpStr string

	if replayFlag {
		// Create Line Protocol Replay String
		// example output:
		//		mydb	(database name)
		//		autogen (retention policy)
		//		cdn.edgecast.http_small.statuscode.other value=0 1486489200000
		if fakeTimeFlag {
			lpStr = lpStr + fmt.Sprintf("%v\n%v\n%v key=\"%v\",value=%v %v\n", dbFlag, rpFlag, tableName, key, value, ftimestamp)
		} else {
			lpStr = lpStr + fmt.Sprintf("%v\n%v\n%v key=\"%v\",value=%v\n", dbFlag, rpFlag, tableName, key, value)
		}
	} else {
		// Create plain Line Protocol String
		// example output:
		//		cdn.edgecast.http_small.statuscode.other value=0 1486489200000
		if fakeTimeFlag {
			lpStr = lpStr + fmt.Sprintf("%v key=\"%v\",value=%v %v\n", tableName, key, value, ftimestamp)
		} else {
			lpStr = lpStr + fmt.Sprintf("%v key=\"%v\",value=%v\n", tableName, key, value)
		}
	}
	return lpStr
}

// Return gzip compressed string
func gzipString(str string) string {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write([]byte(str))
	if err != nil {
		log.Fatal("Could not write to gzip buffer:\n", err)
	}

	err = zw.Flush()
	if err != nil {
		log.Fatal("Could not flush to gzip buffer:\n", err)
	}

	if err := zw.Close(); err != nil {
		log.Fatal("Could not close gzip buffer:\n", err)
	}

	return buf.String()
}

// Write string to disk
func writeFile(str, outfile string) {
	outfile = outdirFlag + filepath.Base(outfile)

	err := os.MkdirAll(outdirFlag, 0755)
	if err != nil {
		log.Fatalln("Not able do create output directory", err)
	}

	f, err := os.Create(outfile)
	if err != nil {
		log.Fatal("Could not create output file:\n", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	_, err = w.WriteString(str)
	if err != nil {
		log.Fatal("Could not write to plain output file:\n", err)
	}

	err = w.Flush()
	if err != nil {
		log.Fatal("Could not flush to plain output file:\n", err)
	}
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		log.Fatalln("Not able to locate filepath: ", err)
	}

	if fi.Mode().IsDir() {
		return true
	}
	return false
}

package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type csvFile struct {
	headerToIndex map[string]int
	indexToHeader map[int]string

	index map[string]int
	rows  [][]string
}

type csvConfig struct {
	hasHeader bool
	comma     rune
	idField   int
}

func loadFile(cfg *csvConfig, name string, readAll bool) (*csvFile, *csv.Reader, error) {
	f, err := os.Open(name)
	if err != nil {
		log.Fatalf("Can't open file %q: %s", name, err)
	}

	r := csv.NewReader(bufio.NewReader(f))
	r.Comma = cfg.comma

	var headerToIndex map[string]int
	var indexToHeader map[int]string
	if cfg.hasHeader {
		headerToIndex = make(map[string]int)
		indexToHeader = make(map[int]string)
		h, err := r.Read()
		if err != nil {
			return nil, nil, fmt.Errorf("can't read header from file %q: %w", name, err)
		}
		for i, s := range h {
			headerToIndex[s] = i
			indexToHeader[i] = s
		}
	}

	var records [][]string
	var index map[string]int
	if readAll {
		index = make(map[string]int)
		records, err = r.ReadAll()
		if err != nil {
			return nil, nil, fmt.Errorf("can't read records from file %q: %w", name, err)
		}
		for i, record := range records {
			index[record[cfg.idField]] = i
		}
	}

	return &csvFile{
		headerToIndex,
		indexToHeader,
		index,
		records,
	}, r, nil
}

func compareFile(cfg *csvConfig, path string, left *csvFile) error {
	right, reader, err := loadFile(cfg, path, false)
	if err != nil {
		return err
	}

	reader.ReuseRecord = true
	var count int = 0
	processedIds := make(map[string]bool)
	addedRecords := 0
	removedRecords := 0
	modifiedFields := make(map[string]int)
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("can't read record from file %q: %w", path, err)
		}
		id := rec[cfg.idField]
		processedIds[id] = true
		idx, ok := left.index[id]
		if !ok {
			addedRecords++
			log.Printf("Added record #%d with ID = %q\n", count+1, id)
		} else {
			leftRec := left.rows[idx]
			if len(leftRec) != len(rec) {
				fmt.Printf(
					"Incompatible record #%d - #%d with ID = %q (%d - %d)\n", idx+1, count+1, id, len(leftRec),
					len(rec),
				)
			} else {
				message := ""
				for i, s := range rec {
					if leftRec[i] != s {
						if message == "" {
							message = fmt.Sprintf("Changed records #%d - #%d with ID = %q:\n", idx+1, count+1, id)
						}
						leftHeader := left.indexToHeader[i]
						rightHeader := right.indexToHeader[i]
						if leftHeader == rightHeader {
							message += fmt.Sprintf("    %q: %q - %q\n", leftHeader, leftRec[i], s)
							modifiedFields[leftHeader]++
						} else {
							message += fmt.Sprintf("    %q: %q - %q: %q\n", leftHeader, leftRec[i], rightHeader, s)
							modifiedFields[fmt.Sprintf("%s - %s", leftHeader, rightHeader)]++
						}

					}
				}
				if message != "" {
					fmt.Print(message)
				}
			}
		}
		count++
	}
	for idx, rec := range left.rows {
		id := rec[cfg.idField]
		if !processedIds[id] {
			removedRecords++
			fmt.Printf("Removed record #%d with ID = %q\n", idx+1, id)
		}
	}
	fmt.Println("-----------------------------------------------------------")
	fmt.Printf("Added %d records\n", addedRecords)
	fmt.Printf("Removed %d records\n", removedRecords)
	fmt.Printf("Changed fields:\n")
	for k, v := range modifiedFields {
		fmt.Printf("    %q: %d\n", k, v)
	}
	return nil
}

func main() {
	commaFlag := flag.String("sep", "|", "CSV file separator")
	idFlag := flag.Int("id", 1, "1-based field index used to uniquely identify CSV record")
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s: [flags] [left path] [right path]\n\nFlags:\n", os.Args[0])

		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()

	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}
	cfg := csvConfig{
		hasHeader: true,
		comma:     []rune(*commaFlag)[0],
		idField:   *idFlag - 1,
	}
	leftName := args[0]
	rightName := args[1]

	left, _, err := loadFile(&cfg, leftName, true)
	if err != nil {
		log.Fatalln(err)
	}

	err = compareFile(&cfg, rightName, left)
	if err != nil {
		log.Fatalln(err)
	}
}

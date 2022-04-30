package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/hippora/autostock/db"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CSVStore struct {
	dirname       string
	concurrentNum int
	fileChan      chan string
	db            db.Querier
}

func NewCSVStore(dirname string, concurrentNum int, db db.Querier) *CSVStore {
	fileInfo, err := ioutil.ReadDir(dirname)
	if err != nil {
		log.Fatalf("can not read dir : %v", dirname)
		return nil
	}
	c := make(chan string, len(fileInfo))
	defer close(c)
	for _, file := range fileInfo {
		c <- file.Name()
	}
	return &CSVStore{
		dirname:       dirname,
		concurrentNum: concurrentNum,
		fileChan:      c,
		db:            db,
	}
}

func (s *CSVStore) Save() {
	var wg sync.WaitGroup
	wg.Add(s.concurrentNum)
	for i := 0; i < s.concurrentNum; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				fileName, open := <-s.fileChan
				if !open {
					break
				}
				fileFullName := path.Join(s.dirname, fileName)
				fmt.Printf("goroutine: %d\t import file:%s\n", i, fileFullName)
				// parse csv and save to db
				file, err := os.Open(fileFullName)
				if err != nil {
					fmt.Print("open file error:", err)
					file.Close()
					continue
				}

				r := csv.NewReader(file)
				r.FieldsPerRecord = 7
				for {
					record, err := r.Read()
					if err == io.EOF {
						break
					}
					if len(record) != r.FieldsPerRecord {
						continue
					}
					if err != nil {
						log.Printf("parse csv error:%v", err)
						continue
					}
					//fmt.Println(record)
					code := strings.TrimSuffix(fileName, path.Ext(fileName))
					tradeDate, err := time.Parse("2006-01-02", record[0])
					if err != nil {
						log.Printf("parse date error:%v", err)
						break
					}
					vol, err := strconv.ParseInt(record[5], 10, 64)
					if err != nil {
						log.Printf("parse Vol error:%v", err)
						break
					}
					arg := db.CreateStockDailyParams{
						Code:      code,
						TradeDate: tradeDate,
						Open:      record[1],
						High:      record[2],
						Low:       record[3],
						Close:     record[4],
						Vol:       vol,
						Amount:    record[6],
					}
					err = s.db.CreateStockDaily(context.Background(), arg)
					if err != nil {
						log.Printf("save csv record error:%v", err)
						break
					}

				}
				file.Close()
			}
			fmt.Printf("goroutine: %d finished\n", i)
		}(i)
	}
	wg.Wait()
}

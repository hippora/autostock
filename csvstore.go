package main

import (
	"context"
	"database/sql"
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
	sqldb         *sql.DB
	query         *db.Queries
}

func NewCSVStore(dirname string, concurrentNum int, sqldb *sql.DB) *CSVStore {
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
		sqldb:         sqldb,
		query:         db.New(sqldb),
	}
}

func (s *CSVStore) execTx(fn func(*db.Queries) error) error {
	tx, err := s.sqldb.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	err = fn(s.query)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}
	return tx.Commit()
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
				// begin transaction
				hasBrief := true
				s.execTx(func(queries *db.Queries) error {
					code := strings.TrimSuffix(fileName, path.Ext(fileName))
					//GetStockBrief
					stockBrief, err := queries.GetStockBrief(context.Background(), code)
					lastTradeDate := stockBrief.LastTradeDate
					if err != nil {
						hasBrief = false
						lastTradeDate, _ = time.Parse("2006-01-02", "1949-10-01")
					}
					for {
						record, err := r.Read()
						if err == io.EOF {
							break
						}
						if len(record) != r.FieldsPerRecord {
							continue
						}
						if err != nil {
							return fmt.Errorf("parse csv error:%v,file:%s", err, fileName)
						}
						//fmt.Println(record)
						tradeDate, err := time.Parse("2006-01-02", record[0])
						if err != nil {
							return fmt.Errorf("parse tradeDate error:%v,file:%s", err, fileName)
						}
						if lastTradeDate.After(tradeDate) {
							continue
						}
						lastTradeDate = tradeDate
						vol, err := strconv.ParseInt(record[5], 10, 64)
						if err != nil {
							return fmt.Errorf("parse vol error:%v,file:%s", err, fileName)
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
						err = queries.CreateStockDaily(context.Background(), arg)
						if err != nil {
							return fmt.Errorf("save stock daily error:%v,file:%s", err, fileName)
						}

					}
					if !hasBrief {
						argBrief := db.CreateStockBriefParams{
							Code:          code,
							LastTradeDate: lastTradeDate,
						}
						err = queries.CreateStockBrief(context.Background(), argBrief)
						if err != nil {
							return fmt.Errorf("save stock brief error:%v,file:%s", err, fileName)
						}
					} else {
						argUpdateBrief := db.UpdateStockBriefParams{
							LastTradeDate: lastTradeDate,
							Code:          code,
						}
						err = queries.UpdateStockBrief(context.Background(), argUpdateBrief)
						if err != nil {
							return fmt.Errorf("update stock brief error:%v,file:%s", err, fileName)
						}
					}
					return nil
				})

				_ = file.Close()
			}

			fmt.Printf("goroutine: %d finished\n", i)
		}(i)
	}
	wg.Wait()
}

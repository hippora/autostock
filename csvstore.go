package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/hippora/autostock/db"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CSVStore struct {
	dirname  string
	workerCh chan struct{}
	sqldb    *sql.DB
	query    *db.Queries
}

func NewCSVStore(dirname string, workers int, sqldb *sql.DB) *CSVStore {
	c := make(chan struct{}, workers)
	//sqldb.SetMaxIdleConns(8)
	return &CSVStore{
		dirname:  dirname,
		workerCh: c,
		sqldb:    sqldb,
		query:    db.New(sqldb),
	}
}

func (s *CSVStore) execTx(fn func(*db.Queries) error) error {
	tx, err := s.sqldb.Begin()
	if err != nil {
		return err
	}
	q := s.query.WithTx(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}
	return tx.Commit()
}

func (s *CSVStore) Save() {
	fileInfo, err := ioutil.ReadDir(s.dirname)
	if err != nil {
		fmt.Printf("can not read dir : %v", s.dirname)
		return
	}
	var wg sync.WaitGroup
	for i, f := range fileInfo {
		s.workerCh <- struct{}{}
		wg.Add(1)
		go func(i int, fileName string) {
			defer wg.Done()
			fmt.Printf("goroutine: %d\t import file:%s\n", i, fileName)
			fileFullName := path.Join(s.dirname, fileName)
			file, err := os.Open(fileFullName)
			defer file.Close()
			if err != nil {
				fmt.Print("open file error:", err)
				return
			}
			// parse csv and save to db
			r := csv.NewReader(file)
			r.FieldsPerRecord = 7
			// begin transaction
			hasBrief := true
			err = s.execTx(func(queries *db.Queries) error {
				code := strings.TrimSuffix(fileName, path.Ext(fileName))
				//GetStockBrief
				stockBrief, err := queries.GetStockBrief(context.Background(), code)
				lastTradeDate := stockBrief.LastTradeDate
				if err != nil {
					hasBrief = false
					lastTradeDate, _ = time.Parse("2006-01-02", "1949-10-01")
				}
				rows := 0
				for {
					record, err := r.Read()
					if err == io.EOF {
						break
					}
					if len(record) != r.FieldsPerRecord {
						continue
					}
					if err != nil {
						return fmt.Errorf("parse csv error:%v", err)
					}
					//fmt.Println(record)
					tradeDate, err := time.Parse("2006-01-02", record[0])
					if err != nil {
						return fmt.Errorf("parse tradeDate error:%v", err)
					}
					if lastTradeDate.After(tradeDate) {
						continue
					}
					lastTradeDate = tradeDate
					vol, err := strconv.ParseInt(record[5], 10, 64)
					if err != nil {
						return fmt.Errorf("parse vol error:%v", err)
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
						return fmt.Errorf("save stock daily error:%v", err)
					}
					//fmt.Printf("rows:%d,record:%v\n", rows, arg)
					rows++
				}
				if !hasBrief {
					argBrief := db.CreateStockBriefParams{
						Code:          code,
						LastTradeDate: lastTradeDate,
					}
					err = queries.CreateStockBrief(context.Background(), argBrief)
					if err != nil {
						return fmt.Errorf("save stock brief error:%v", err)
					}
				}
				if hasBrief && rows > 0 {
					argUpdateBrief := db.UpdateStockBriefParams{
						LastTradeDate: lastTradeDate,
						Code:          code,
					}
					err = queries.UpdateStockBrief(context.Background(), argUpdateBrief)
					if err != nil {
						return fmt.Errorf("update stock brief error:%v", err)
					}
				}
				return nil
			})
			if err != nil {
				fmt.Printf("goroutine: %d\t import file:%s failed,err:%v\n", i, fileName, err)
			} else {
				fmt.Printf("goroutine: %d\t import file:%s success\n", i, fileName)
			}
			<-s.workerCh
		}(i, f.Name())
		i++
	}
	close(s.workerCh)
	wg.Wait()
}

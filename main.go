package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/hippora/autostock/db"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/spf13/viper"
	"log"
	"time"
)

func main() {
	concurrentNum := flag.Int("p", 8, "max concurrent number")
	flag.Parse()
	viper.AddConfigPath(".")
	viper.SetConfigName("app")
	viper.SetConfigType("toml")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("read config file error:", err)
	}
	conn, err := sql.Open("pgx", viper.GetString("dbsource"))
	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}
	defer conn.Close()
	store := db.New(conn)
	start := time.Now()
	csvStore := NewCSVStore(viper.GetString("csvdir"), *concurrentNum, store)
	csvStore.Save()
	fmt.Printf("elapsed time:%s\n", time.Since(start).String())
}

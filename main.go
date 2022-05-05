package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pelletier/go-toml"
	"log"
	"time"
)

func main() {
	workerNum := flag.Int("p", 8, "max concurrent number")
	flag.Parse()
	config, err := toml.LoadFile("app.toml")
	if err != nil {
		log.Fatal("read config file error:", err)
	}
	conn, err := sql.Open("pgx", config.Get("dbsource").(string))
	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}
	defer conn.Close()

	start := time.Now()
	csvStore := NewCSVStore(config.Get("csvdir").(string), *workerNum, conn)
	csvStore.Save()
	fmt.Printf("elapsed time:%s\n", time.Since(start).String())
}

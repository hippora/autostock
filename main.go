package main

import (
	"database/sql"
	"github.com/hippora/autostock/db"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/spf13/viper"
	"log"
)

func main() {
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

	csvStore := NewCSVStore(viper.GetString("csvdir"), 4, store)
	csvStore.Save()

}

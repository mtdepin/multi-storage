package db

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"mtcloud.com/mtstorage/pkg/logger"
)

type DBconfig struct {
	DbName     string
	Url        string
	DbUser     string
	DbPassword string
}

type Database struct {
	DB *gorm.DB
}

var database *Database

func InitDb(config DBconfig) *Database {
	logger.Info(" InitDb")
	database = &Database{}
	str := fmt.Sprintf("%s:%s@%s/%s?charset=utf8mb4&parseTime=True&loc=Local", config.DbUser, config.DbPassword, config.Url, config.DbName)
	db, err := gorm.Open("mysql", str)
	if err != nil {
		logger.Error("connect to database faild :", err.Error())
		panic(err)
	}

	db = db.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4 auto_increment=1")
	db.DB().SetMaxIdleConns(200)
	db.DB().SetMaxOpenConns(500)
	//debug mode
	db.LogMode(true)

	gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
		return "t_" + defaultTableName
	}

	database.DB = db.Debug()
	return database
}

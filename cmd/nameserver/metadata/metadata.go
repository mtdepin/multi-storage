package metadata

import (
	"fmt"
	"mtcloud.com/mtstorage/pkg/crypto"
	"mtcloud.com/mtstorage/pkg/db"
	"mtcloud.com/mtstorage/pkg/logger"
)

type MetaData struct {
	db *db.Database
	//XXX
	//XXX
}

func (BucketInfo) TableName() string {
	return BucketTable
}

func (BucketExternal) TableName() string {
	return BucketExtTable
}

func (ObjectInfo) TableName() string {
	return ObjectTable
}

func (ObjectHistoryInfo) TableName() string {
	return ObjectHistoryTable
}

func (ObjectChunkInfo) TableName() string {
	return ObjectCidTable
}

var mtMetadata = &MetaData{}

func InitMetadata(c db.DBconfig) {
	logger.Info("init meta db")
	//decrypto db password
	c.DbPassword = crypto.DecryptLocalPassword(c.DbPassword)
	c.Url = fmt.Sprintf("tcp(%s)", c.Url)

	db := db.InitDb(c)

	if !db.DB.HasTable(&BucketInfo{}) {
		if err := db.DB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&BucketInfo{}).Error; err != nil {
			logger.Error("create bucket info table failed:", err)
			return
		}
	}

	if !db.DB.HasTable(&BucketExternal{}) {
		if err := db.DB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&BucketExternal{}).Error; err != nil {
			logger.Error("create bucket externel info table failed:", err)
			return
		}
	}

	if !db.DB.HasTable(&ObjectInfo{}) {
		if err := db.DB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&ObjectInfo{}).Error; err != nil {
			logger.Error("create object info table failed:", err)
			return
		}
	}

	if !db.DB.HasTable(&ObjectHistoryInfo{}) {
		if err := db.DB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&ObjectHistoryInfo{}).Error; err != nil {
			logger.Error("create object history info table failed:", err)
			return
		}
	}

	if !db.DB.HasTable(&ObjectChunkInfo{}) {
		if err := db.DB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&ObjectChunkInfo{}).Error; err != nil {
			logger.Error("create object cid info table failed:", err)
			return
		}
	}
	//auto migrate
	/*
		gorm.DefaultTableNameHandler= func(db *gorm.DB, defaultTableName string) string {
			if strings.HasPrefix(defaultTableName,"t_"){
				return strings.TrimPrefix(defaultTableName,"t_")
			}
			return defaultTableName
		}
	*/

	db.DB.AutoMigrate(&BucketInfo{})
	db.DB.AutoMigrate(&BucketExternal{})
	db.DB.AutoMigrate(&ObjectInfo{})
	db.DB.AutoMigrate(&ObjectHistoryInfo{})
	db.DB.AutoMigrate(&ObjectChunkInfo{})

	mtMetadata.db = db
}

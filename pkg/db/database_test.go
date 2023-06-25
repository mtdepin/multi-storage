package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var DBName = "mt_iam"
var DbUrl = "tcp(192.168.1.181:3306)"
var DbUser = "root"
var DbPassword = "123456"

func TestDb(t *testing.T) {
	db := InitDb(&DBconfig{
		Name:     DBName,
		Url:      DbUrl,
		User:     DbUser,
		Password: DbPassword,
	})

	assert.NotNil(t, db, "db init storageerror")

}

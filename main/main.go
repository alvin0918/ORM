package main

import (
	"github.com/alvin0918/ORM"
)

func main()  {
	_, _ = ORM.DBConfig.Alias("").TableName("").Field("").Select()
}
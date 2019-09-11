package main

import (
	"fmt"
	"github.com/alvin0918/ORM"
)

func main()  {

	var (
		field map[string]string
	)

	field = make(map[string]string)

	field["A"] = "B"

	a, _ := ORM.DBConfig.Where("1=1", "and").TableName("luffy_teacher").IsPrintSql(true).Find()

	for k,v := range a {
		fmt.Println(k)
		fmt.Println(v)
	}
}
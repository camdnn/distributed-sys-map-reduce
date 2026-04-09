package main

import (
	"fmt"
	"log"
	"os"
)

const M = 9
const R = 3
const filename = "../dao.txt"

func main() {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("os.OpenFile", err)
	}
	defer file.Close()

	Coordinator(M, R, file)

	fmt.Println("Successfully ran file")

}

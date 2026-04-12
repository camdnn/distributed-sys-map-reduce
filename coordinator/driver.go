package main

import (
	"fmt"
	"log"
	"os"
)

const M int = 100
const R int = 10
const filename = "../warandpeace.txt"

func main() {
	fmt.Println("DRIVER:")
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("os.OpenFile", err)
	}
	defer file.Close()

	Coordinator(M, R, file)

	fmt.Println("Successfully ran file")

	for !Done() {

	}

}

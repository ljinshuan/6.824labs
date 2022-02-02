package main

import (
	"fmt"
	"os"
)

func main() {

	for idx, arg := range os.Args {

		fmt.Printf("arg %v %v\n", idx, arg)
	}
}

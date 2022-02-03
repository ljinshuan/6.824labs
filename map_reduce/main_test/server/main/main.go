package main

import (
	"6.824labs/map_reduce/main_test/server"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

func main() {

	rpcServer := &server.RpcServer{}
	rpc.Register(rpcServer)
	rpc.HandleHTTP()
	os.Remove("zhisu_test")
	listen, err := net.Listen("unix", "zhisu_test")
	if err != nil {
		fmt.Println(err)
		log.Fatalln(err)
	}
	http.Serve(listen, nil)
}

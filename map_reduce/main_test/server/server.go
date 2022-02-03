package server

import "fmt"

type Request struct {
	Msg string
}

type Response struct {
	Msg string
}

type RpcServer struct {
}

func (s *RpcServer) Hello(request *Request, response *Response) error {

	fmt.Println(request.Msg)

	response.Msg = "hello " + request.Msg
	return nil
}

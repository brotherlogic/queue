package main

import (
	"fmt"
	"reflect"

	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"
)

func (s *Server) runRPC(ctx context.Context, service, method string, payload proto.Message) (*proto.Message, error) {
	resolution := "recordcollection"
	conn, err := s.FDial(resolution)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	clientf, err := s.buildClient(service)
	if err != nil {
		return nil, err
	}

	fc := reflect.ValueOf(clientf)
	pieces := fc.Call([]reflect.Value{reflect.ValueOf(conn)})
	methodFunc := reflect.ValueOf(pieces[0].Interface()).MethodByName(method).Interface()

	results := reflect.ValueOf(methodFunc).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(payload)})

	err = nil
	if results[1].Interface() != nil {
		return nil, results[1].Interface().(error)

	}

	return results[0].Interface().(*proto.Message), nil
}

func (s *Server) buildClient(service string) (interface{}, error) {
	if val, ok := s.cmap[service]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("Not quite implemented")
}

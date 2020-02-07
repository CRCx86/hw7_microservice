package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"time"
)

type HandlerAdmin struct {
	ctx context.Context
	AdminServer

	LoggingChannel       chan *Event
	LoggingListenChannel chan chan *Event
	LoggingListener      []chan *Event

	StatChannel       chan *Stat
	StatListenChannel chan chan *Stat
	StatListener      []chan *Stat
}

func (h *HandlerAdmin) Logging(nothing *Nothing, server Admin_LoggingServer) error {

	ch := make(chan *Event, 0)
	h.LoggingListenChannel <- ch
	for {
		select {
		case c := <-ch:
			err := server.Send(c)
			if err != nil {
				return err
			}
		case <-h.ctx.Done():
			return nil
		}
	}

}

func (h *HandlerAdmin) Statistics(stat *StatInterval, server Admin_StatisticsServer) error {

	ch := make(chan *Stat, 0)
	h.StatListenChannel <- ch

	t := time.NewTicker(time.Second * time.Duration(stat.IntervalSeconds))
	s := &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}

	for {
		select {
		case stat := <-ch:
			for k, v := range stat.ByMethod {
				s.ByMethod[k] += v
			}
			for k, v := range stat.ByConsumer {
				s.ByConsumer[k] += v
			}
		case <-t.C:
			err := server.Send(s)
			if err != nil {
				return err
			}
			s = &Stat{
				ByMethod:   make(map[string]uint64),
				ByConsumer: make(map[string]uint64),
			}
		case <-h.ctx.Done():
			return nil
		}
	}
}

type HandlerBiz struct {
	BizServer
}

func (*HandlerBiz) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (*HandlerBiz) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (*HandlerBiz) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

type Data map[string][]string

type Server struct {
	data Data
	HandlerAdmin
	HandlerBiz
}

func StartMyMicroservice(ctx context.Context, addr string, data string) error {

	server := &Server{}
	server.ctx = ctx

	e := json.Unmarshal([]byte(data), &server.data)
	if e != nil {
		return e
	}

	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln("can't listen port", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(server.unaryInterceptor),
		grpc.StreamInterceptor(server.streamInterceptor))

	RegisterAdminServer(grpcServer, server)
	RegisterBizServer(grpcServer, server)

	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		done := ctx.Done()
		select {
		case <-done:
			grpcServer.Stop()
		}
	}()

	server.LoggingChannel = make(chan *Event, 0)
	server.LoggingListenChannel = make(chan chan *Event, 0)
	go func() {
		for {
			select {
			case c := <-server.LoggingListenChannel:
				server.LoggingListener = append(server.LoggingListener, c)
			case e := <-server.LoggingChannel:
				for _, ch := range server.LoggingListener {
					ch <- e
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	server.StatChannel = make(chan *Stat, 0)
	server.StatListenChannel = make(chan chan *Stat, 0)
	go func() {
		for {
			select {
			case c := <-server.StatListenChannel:
				server.StatListener = append(server.StatListener, c)
			case s := <-server.StatChannel:
				for _, ch := range server.StatListener {
					ch <- s
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return err

}

func (h *Server) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	if ok := h.validate(ctx, info.FullMethod); ok != nil {
		return nil, ok
	}

	in, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "error")
	}

	h.LoggingChannel <- &Event{
		Consumer: in["consumer"][0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}

	h.StatChannel <- &Stat{
		ByMethod:   map[string]uint64{info.FullMethod: 1},
		ByConsumer: map[string]uint64{in["consumer"][0]: 1},
	}

	return handler(ctx, req)
}

func (h *Server) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {

	if ok := h.validate(ss.Context(), info.FullMethod); ok != nil {
		return ok
	}

	in, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Errorf(codes.Unauthenticated, "error")
	}

	h.LoggingChannel <- &Event{
		Consumer: in["consumer"][0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}

	h.StatChannel <- &Stat{
		ByMethod:   map[string]uint64{info.FullMethod: 1},
		ByConsumer: map[string]uint64{in["consumer"][0]: 1},
	}

	return handler(srv, ss)

}

func (h *Server) validate(ctx context.Context, method string) error {

	in, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "errors metadata")
	}

	methods, ok := in["consumer"]
	if !ok {
		return status.Errorf(codes.Unauthenticated, "metadata doesn't have consumer")
	}

	message, ok := h.data[methods[0]]
	if !ok {
		return status.Errorf(codes.Unauthenticated, "metadata doesn't have allowed path")
	}

	split := strings.Split(method, "/")
	if len(split) < 3 {
		return status.Errorf(codes.Unauthenticated, "metadata has got wrong method ")
	}

	p, m := split[1], split[2]
	isImplement := false
	for _, v := range message {

		s := strings.Split(v, "/")

		if len(s) != 3 {
			continue
		}

		aP, aM := s[1], s[2]

		if p != aP {
			continue
		}

		if m == aM || aM == "*" {
			isImplement = true
			break
		}
	}

	if !isImplement {
		return status.Errorf(codes.Unauthenticated, "metadata has got not allowed method")
	}

	return nil

}

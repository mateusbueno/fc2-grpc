package main

import(
	"context"
	"fmt"
	"log"
	"io"
	"time"
	"github.com/mateusbueno/fc2-grpc/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to gRPC server: %v", err)
	}
	defer connection.Close()

	client := pb.NewUserServiceClient(connection)
	//AddUser(client)
	//AddUserVerbose(client)
	//AddUsers(client)
	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id: "0",
		Name: "João da Silva",
		Email: "joao@silva.com",
	}

	res, err := client.AddUser(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id: "0",
		Name: "João da Silva",
		Email: "joao@silva.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	for {
		stream, err := responseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive the msg: %v", err)
		}
		fmt.Println("Status:", stream.Status, "-", stream.GetUser())
	}
	
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id: "m1",
			Name: "Mateus",
			Email: "mateusob@gmail.com",
		},
		&pb.User{
			Id: "m2",
			Name: "Mateus 2",
			Email: "mateusob2@gmail.com",
		},
		&pb.User{
			Id: "m3",
			Name: "Mateus 3",
			Email: "mateusob3@gmail.com",
		},
		&pb.User{
			Id: "m4",
			Name: "Mateus 4",
			Email: "mateusob4@gmail.com",
		},
		&pb.User{
			Id: "m5",
			Name: "Mateus 5",
			Email: "mateusob5@gmail.com",
		},
	}
	stream, err := client.AddUsers(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)

}

func AddUserStreamBoth(client pb.UserServiceClient) {
	stream, err := client.AddUserStreamBoth(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id: "m1",
			Name: "Mateus",
			Email: "mateusob@gmail.com",
		},
		&pb.User{
			Id: "m2",
			Name: "Mateus 2",
			Email: "mateusob2@gmail.com",
		},
		&pb.User{
			Id: "m3",
			Name: "Mateus 3",
			Email: "mateusob3@gmail.com",
		},
		&pb.User{
			Id: "m4",
			Name: "Mateus 4",
			Email: "mateusob4@gmail.com",
		},
		&pb.User{
			Id: "m5",
			Name: "Mateus 5",
			Email: "mateusob5@gmail.com",
		},
	}

	wait := make(chan int)

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user:", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	go func () {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving data: %v", err)
				break
			}
			fmt.Printf("Recebendo user %v com status: %v \n", res.GetUser().GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait

}

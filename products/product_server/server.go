package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"products/productpb"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var collection *mongo.Collection

type product struct {
	Id    primitive.ObjectID `bson: "_id, omitempty"`
	Name  string             `bson: "name"`
	Price float64            `bson: "price"`
}

type server struct {
	productpb.UnimplementedProductServiceServer
}

func (*server) GetProduct(ctx context.Context, req *productpb.GetProductRequest) (*productpb.GetProductResponse, error) {

	productId := req.GetId()

	oid, err := primitive.ObjectIDFromHex(productId)

	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse ID"),
		)
	}

	data := &product{}

	filter := bson.M{"_id": oid}

	res := collection.FindOne(context.Background(), filter)

	if err := res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find the product, %v", err),
		)
	}

	data.Id = oid

	return &productpb.GetProductResponse{
		Product: dbToProductPb(data),
	}, nil

}

func (*server) CreateProduct(ctx context.Context, req *productpb.CreateProductRequest) (*productpb.CreateProductResponse, error) {

	prod := req.GetProduct()

	data := product{
		Name:  prod.GetName(),
		Price: prod.GetPrice(),
	}

	res, err := collection.InsertOne(context.Background(), data)

	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal error, %v", err),
		)
	}

	oid, ok := res.InsertedID.(primitive.ObjectID)

	if !ok {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal convert id, %v", err),
		)
	}

	return &productpb.CreateProductResponse{
		Product: &productpb.Product{
			Id:    oid.Hex(),
			Name:  prod.GetName(),
			Price: prod.GetPrice(),
		},
	}, nil
}

func (*server) UpdateProduct(ctx context.Context, req *productpb.UpdateProductRequest) (*productpb.UpdateProductResponse, error) {

	prod := req.GetProduct()

	oid, err := primitive.ObjectIDFromHex(prod.GetId())

	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse ID"),
		)
	}

	data := &product{}
	filter := bson.M{"_id": oid}

	res := collection.FindOne(context.Background(), filter)

	if err := res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find the product, %v", err),
		)
	}

	data.Id = oid
	data.Name = prod.GetName()
	data.Price = prod.GetPrice()

	_, updateError := collection.ReplaceOne(context.Background(), filter, data)

	if updateError != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Cannot updated the product, %v", updateError),
		)
	}

	return &productpb.UpdateProductResponse{
		Product: dbToProductPb(data),
	}, nil

}

func (*server) ListProducts(req *productpb.ListProductsRequest, stream productpb.ProductService_ListProductsServer) error {

	fmt.Println("List Products")

	listP, err := collection.Find(context.Background(), primitive.D{{}})

	if err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal error %v", err),
		)
	}

	defer listP.Close(context.Background())

	for listP.Next(context.Background()) {
		data := &product{}

		err := listP.Decode(data)

		if err != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintf("Error decoding data product %v", err),
			)
		}

		stream.Send(&productpb.ListProductsResponse{
			Product: dbToProductPb(data),
		})
	}

	if err := listP.Err(); err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal error %v", err),
		)
	}

	return nil

}

func dbToProductPb(data *product) *productpb.Product {

	return &productpb.Product{
		Id:    data.Id.Hex(),
		Name:  data.Name,
		Price: data.Price,
	}
}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	/*
	   Connect to my cluster
	*/
	addr := "mongodb://localhost:27017"

	client, err := mongo.NewClient(options.Client().ApplyURI(addr))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	/*
	   List databases
	*/
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)

	collection = client.Database("productsdb").Collection("products")

	fmt.Println("Product Service")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")

	if err != nil {
		log.Fatalf("Failed to listen, %v", err)
	}

	s := grpc.NewServer()

	productpb.RegisterProductServiceServer(s, &server{})

	go func() {
		fmt.Println("Starting server!")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to server, %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)

	signal.Notify(ch, os.Interrupt)

	<-ch
	fmt.Println("Stopping the server!")
	s.Stop()

	fmt.Println("Closing the listener")
	lis.Close()

	// client.Disconnect(context.TODO())

	fmt.Println("Bye!")

}

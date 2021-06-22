package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"products/productpb"

	"google.golang.org/grpc"
)

func main() {

	fmt.Println("Starting product service!")

	con, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect %v \n", err)
	}

	defer con.Close()

	c := productpb.NewProductServiceClient(con)

	product := &productpb.Product{
		Name:  "Portátil Acer",
		Price: 2000000,
	}

	createdProduct, err := c.CreateProduct(context.Background(), &productpb.CreateProductRequest{
		Product: product,
	})

	if err != nil {
		log.Fatalf("Failed to create product %v \n", err)
	}

	fmt.Printf("Product created %v \n", createdProduct)

	// Get Product

	fmt.Println("Get Product")

	prodId := createdProduct.GetProduct().GetId()

	getProdRequest := &productpb.GetProductRequest{Id: prodId}

	getProdResponse, getProdErr := c.GetProduct(context.Background(), getProdRequest)

	if getProdErr != nil {
		log.Fatalf("failed to getting product %v \n", getProdErr)
	}

	fmt.Printf("Product gotten: %v \n", getProdResponse)

	// Update Product

	fmt.Println("Update Product")
	newProduct := &productpb.Product{
		Id:    prodId,
		Name:  "New computer: HP",
		Price: 1500000,
	}

	updateResponse, updateErr := c.UpdateProduct(
		context.Background(),
		&productpb.UpdateProductRequest{Product: newProduct},
	)

	if updateErr != nil {
		fmt.Printf("Error happened while updating %v \n", updateErr)
	}

	fmt.Printf("Product was updated %v \n", updateResponse)

	// List Product
	fmt.Println("List Product")

	stream, err := c.ListProducts(context.Background(), &productpb.ListProductsRequest{})

	if err != nil {
		log.Fatalf("Error calling list product %v", err)
	}

	for {
		res, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("Error receiving product %v", err)
		}

		fmt.Println(res.GetProduct())
	}

}

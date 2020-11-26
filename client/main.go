package main

import (
	// "strconv"
	"fmt"
	"github.com/ritwiksamrat/newkafka/proto"
	"log"
	"net/http"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)
func main(){
	conn, err:= grpc.Dial("localhost:4040", grpc.WithInsecure())
	if err!=nil{
		panic(err)
	}
	client:= proto.NewProducerServiceClient(conn)

	g:=gin.Default()
	g.GET("/producer/:a", func(ctx *gin.Context){
		a := ctx.Param("a")

		req := &proto.Request{Username: string(a)}
		if response, err := client.Producer(ctx, req); err == nil {
			ctx.JSON(http.StatusOK, gin.H{
				"result": fmt.Sprint(response.Result),
			})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})
	
	
	if err:=g.Run(":8080"); err!=nil{
		log.Fatalf("Failed	to run server: %v", err)
	}
}

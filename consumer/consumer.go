package main

import (
	"fmt"
	"log"
	"context"
	"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (  
    username = "newuser"
    password = "password"
    hostname = "127.0.0.1:3306"
    dbname   = "kafka"
)

func main() {
	db, err := dbConnection()
    if err != nil {
        log.Printf("Error %s when getting db connection", err)
        return
    }
    defer db.Close()
    log.Printf("Successfully connected to database")

    
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"sampleTopic"}, nil)

	for{
		msg, err := c.ReadMessage(-1)
		
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))		
					
			var username= string(msg.Value)
			err=insertMemberTable(db, username)
			if err != nil {
				log.Printf("Insert into members table failed with error %s", err)
				return
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}	
	}
	c.Close()
}


func dsn(dbName string) string {  
    return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {  
    db, err := sql.Open("mysql", dsn(""))
    if err != nil {
        log.Printf("Error %s when opening DB\n", err)
        return nil, err
    }
    //defer db.Close()

    ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancelfunc()

    db.Close()

	db, err = sql.Open("mysql", dsn(dbname))
    if err != nil {
        log.Printf("Error %s when opening DB", err)
        return nil, err
    }
    //defer db.Close()

    db.SetMaxOpenConns(20)
    db.SetMaxIdleConns(20)
    db.SetConnMaxLifetime(time.Minute * 5)

    ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
    defer cancelfunc()
    err = db.PingContext(ctx)
    if err != nil {
        log.Printf("Errors %s pinging DB", err)
        return nil, err
    }
    log.Printf("Connected to DB %s successfully\n", dbname)
    return db, nil
}

func insertMemberTable(db *sql.DB, user_name string) error{
   
    stmt, err := db.Prepare("INSERT INTO members (username) values (?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(user_name)

	if err != nil {
		fmt.Print(err.Error())
    }
    return nil
}
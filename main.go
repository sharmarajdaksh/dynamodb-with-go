package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var tableRegion = "ap-south-1"
var dynamoDBEndpoint = "http://localhost:4566"
var awsID = "1"
var awsSecret = "2"
var awsToken = "3"
var tableName = "test-table"

type pair struct {
	Key   string
	Value string
}

func describeTable(svc *dynamodb.DynamoDB, tbl string) (*dynamodb.TableDescription, error) {
	req := &dynamodb.DescribeTableInput{
		TableName: aws.String(tbl),
	}
	result, err := svc.DescribeTable(req)
	if err != nil {
		return nil, err
	}
	table := result.Table
	return table, nil
}

func putKeyValueItem(svc *dynamodb.DynamoDB, tbl string, p pair) error {
	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		return err
	}
	ip := &dynamodb.PutItemInput{ Item:      av,
		TableName: aws.String(tbl),
	}

	_, err = svc.PutItem(ip)

	if err != nil {
		return err
	}

	return nil
}

func getAllItems(svc *dynamodb.DynamoDB, tbl string) (*[]pair, error) {
	params := &dynamodb.ScanInput{
		TableName: aws.String(tbl),
	}

	rs, err := svc.Scan(params)
	if err != nil {
		return nil, err
	}

	obj := []pair{}
	err = dynamodbattribute.UnmarshalListOfMaps(rs.Items, &obj)
	if err != nil {
		return nil, err
	}

	return &obj, nil
}

func getItemForKey(svc *dynamodb.DynamoDB, tbl string, key string) (*pair, error) {
	res, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tbl),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if res.Item == nil {
		// No Items found
		return nil, nil
	}

	np := pair{}
	err = dynamodbattribute.UnmarshalMap(res.Item, &np)
	if err != nil {
		return nil, err
	}

	return &np, nil
}

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(tableRegion),
		Endpoint:    aws.String(dynamoDBEndpoint),
		Credentials: credentials.NewStaticCredentials(awsID, awsSecret, awsToken),
	})
	if err != nil {
		log.Panicln("Failed to create Dynamo DB session")
	}

	svc := dynamodb.New(sess)

	dscr, err := describeTable(svc, tableName)
	if err != nil {
		log.Panicln("Error while getting table description: ", err.Error())
	}
	fmt.Println("Table Description: ", dscr)

	p := pair{
		Key:   "testKeyP",
		Value: "testValueP",
	}

	err = putKeyValueItem(svc, tableName, p)
	if err != nil {
		log.Panicln("Failed to put value: ", err.Error())
	}

	q := pair{
		Key:   "testKeyQ",
		Value: "testValueQ",
	}

	err = putKeyValueItem(svc, tableName, q)
	if err != nil {
		log.Panicln("Failed to put value: ", err.Error())
	}

	ps, err := getAllItems(svc, tableName)
	if err != nil {
		log.Panicln("Failed to get all Items: ", err.Error())
	}
	fmt.Println("Table Items: ", ps)

	np, err := getItemForKey(svc, tableName, "testKeyQ")
	if err != nil {
		log.Panicln("Failed to get Item: ", err.Error())
	}
	if np == nil {
		log.Println("No Item found for key testKeyQ")
		return
	}

	fmt.Println("Retrieved Item for key testKeyQ", np)
}

package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"os"
	"strings"
	"time"
)

var (
	services = []string{"dynamodb", "dax"}
	commands = []string{"create-table", "put-item", "get-item", "query", "scan", "delete-table"}
	
	// flag args
	service = flag.String("service", "dynamodb", "dax | dynamodb")
	region = flag.String("region", "us-west-2", "aws region")
	endpoint = flag.String("endpoint", "", "dax cluster endpoint")
	command = flag.String("command", "", strings.Join(commands, " | "))
	verbose = flag.Bool("verbose", false, "verbose output")
)

const (
	table      = "TryDaxGoTable"
	keyPrefix  = "key"
	valPrefix  = "val"
	pkMax      = 10
	skMax      = 10
	iterations = 25
)

func main() {
	flag.Parse()
	if err := validate(); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("invalid input: %v\n", err))
		return
	}

	client, err := initClient()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to init client: %v\n", err))
		return
	}

	if err = executeCommand(client); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to execute command: %v\n", err))
	}
}

func executeCommand(client dynamodbiface.DynamoDBAPI) error {
	switch *command {
	case "create-table":
		return executeCreateTable(client)
	case "put-item":
		return executePutItem(client)
	case "get-item":
		return executeGetItem(client)
	case "query":
		return executeQuery(client)
	case "scan":
		return executeScan(client)
	case "delete-table":
		return executeDeleteTable(client)
	}
	return fmt.Errorf("unknown command %s", *command)
}

func executeCreateTable(client dynamodbiface.DynamoDBAPI) error {
	in := &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: aws.String(dynamodb.KeyTypeHash)},
			{AttributeName: aws.String("sk"), KeyType: aws.String(dynamodb.KeyTypeRange)},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("sk"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeN)},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(100),
			WriteCapacityUnits: aws.Int64(100),
		},
	}
	out, err := client.CreateTable(in)
	if err != nil {
		return err
	}
	writeVerbose(out)
	return nil
}

func executePutItem(client dynamodbiface.DynamoDBAPI) error {
	for i := 0; i < pkMax; i++ {
		for j := 0; j < skMax; j++ {
			item := map[string]*dynamodb.AttributeValue{
				"pk":    {S: aws.String(fmt.Sprintf("%s_%d", keyPrefix, i))},
				"sk":    {N: aws.String(fmt.Sprintf("%d", j))},
				"value": {S: aws.String(fmt.Sprintf("%s_%d_%d", valPrefix, i, j))},
			}
			in := &dynamodb.PutItemInput{
				TableName: aws.String(table),
				Item:      item,
			}
			out, err := client.PutItem(in)
			if err != nil {
				return err
			}
			writeVerbose(out)
		}
	}
	return nil
}

func executeGetItem(client dynamodbiface.DynamoDBAPI) error {
	st := time.Now()
	for c := 0; c < iterations; c++ {
		for i := 0; i < pkMax; i++ {
			for j := 0; j < skMax; j++ {
				key := map[string]*dynamodb.AttributeValue{
					"pk": {S: aws.String(fmt.Sprintf("%s_%d", keyPrefix, i))},
					"sk": {N: aws.String(fmt.Sprintf("%d", j))},
				}
				in := &dynamodb.GetItemInput{
					TableName: aws.String(table),
					Key:       key,
				}
				out, err := client.GetItem(in)
				if err != nil {
					return err
				}
				writeVerbose(out)
			}
		}
	}
	d := time.Since(st)
	os.Stdout.WriteString(fmt.Sprintf("Total Time: %v, Avg Time: %v\n", d, d/iterations))
	return nil
}

func executeQuery(client dynamodbiface.DynamoDBAPI) error {
	st := time.Now()
	for c := 0; c < iterations; c++ {
		in := &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String("pk = :pkval and sk between :skval1 and :skval2"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":pkval":  {S: aws.String(fmt.Sprintf("%s_%d", keyPrefix, 5))},
				":skval1": {N: aws.String(fmt.Sprintf("%d", 2))},
				":skval2": {N: aws.String(fmt.Sprintf("%d", 9))},
			},
		}
		out, err := client.Query(in)
		if err != nil {
			return err
		}
		writeVerbose(out)
	}
	d := time.Since(st)
	os.Stdout.WriteString(fmt.Sprintf("Total Time: %v, Avg Time: %v\n", d, d/iterations))
	return nil
}

func executeScan(client dynamodbiface.DynamoDBAPI) error {
	st := time.Now()
	for c := 0; c < iterations; c++ {
		in := &dynamodb.ScanInput{TableName: aws.String(table)}
		out, err := client.Scan(in)
		if err != nil {
			return err
		}
		writeVerbose(out)
	}
	d := time.Since(st)
	os.Stdout.WriteString(fmt.Sprintf("Total Time: %v, Avg Time: %v\n", d, d/iterations))
	return nil
}

func executeDeleteTable(client dynamodbiface.DynamoDBAPI) error {
	in := &dynamodb.DeleteTableInput{TableName: aws.String(table)}
	out, err := client.DeleteTable(in)
	if err != nil {
		return err
	}
	writeVerbose(out)
	return nil
}

func writeVerbose(o interface{}) {
	if verbose != nil && *verbose {
		os.Stdout.WriteString(fmt.Sprintf("%v\n", o))
	}
}

func initClient() (dynamodbiface.DynamoDBAPI, error) {
	if *service == "dax" {
		return daxClient(*endpoint, *region)
	}
	return ddbClient(*region)
}

func ddbClient(region string) (dynamodbiface.DynamoDBAPI, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		return nil, err
	}
	return dynamodb.New(sess), nil
}

func daxClient(endpoint, region string) (dynamodbiface.DynamoDBAPI, error) {
	cfg := dax.DefaultConfig()
	cfg.HostPorts = []string{endpoint}
	cfg.Region = region
	return dax.New(cfg)
}

func validate() error {
	if service == nil || !contains(*service, services) {
		return fmt.Errorf("service should be one of [%s]", strings.Join(services, " | "))
	}
	if command == nil || !contains(*command, commands) {
		return fmt.Errorf("command should be one of [%s]", strings.Join(commands, " | "))
	}
	if *service == "dax" {
		if endpoint == nil || len(*endpoint) == 0 {
			return fmt.Errorf("endpoint should be set for 'dax' service")
		}
		if *command == "create-table" || *command == "delete-table" {
			return fmt.Errorf("service 'dax' does not support table operations, use service 'dynamodb'")
		}
	}
	return nil
}

func contains(needle string, haystack []string) bool {
	for _, v := range haystack {
		if needle == v {
			return true
		}
	}
	return false
}

package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"os"
	"strings"
	"time"
)

type tableClient interface {
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DeleteTable(*dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
}

type itemClient interface {
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	Query(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

var services = []string{"dynamodb", "dax"}

var commandMap = map[string]func() error{
	"create-table": executeCreateTable,
	"delete-table": executeDeleteTable,
	"put-item":     executePutItem,
	"get-item":     executeGetItem,
	"query":        executeQuery,
	"scan":         executeScan,
}

func listOfKeys(m map[string]func() error) []string {
	keys := make([]string, len(m))
	i := 0
	for key := range m {
		keys[i] = key
		i++
	}
	return keys
}

var commandsMsg = strings.Join(listOfKeys(commandMap), " | ")

var service = flag.String("service", "dynamodb", "dax | dynamodb")
var region *string
var endpoint = flag.String("endpoint", "", "dax cluster endpoint")
var command = flag.String("command", "", commandsMsg)
var verbose = flag.Bool("verbose", false, "verbose output")

const (
	table      = "TryDaxGoTable"
	keyPrefix  = "key"
	valPrefix  = "val"
	pkMax      = 10
	skMax      = 10
	iterations = 25
)

func main() {
	if err := initializeOptions(); err != nil {
		os.Exit(1)
	}

	if err := commandMap[*command](); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to execute command: %v\n", err))
		os.Exit(1)
	}
}

func initializeOptions() error {
	// Detect region from the EC2 metadata service
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return err
	}
	md := ec2metadata.New(sess)
	detectedRegion, err := md.Region()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Unable to detect region: %v\n", err))
		return err
	}
	// Override detected region from the command line
	region = flag.String("region", detectedRegion, "AWS region")

	flag.Parse()

	if err := validate(); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("invalid input: %v\n", err))
		return err
	}
	return nil
}

func executeCreateTable() error {
	client, err := initTableClient()
	if err != nil {
		return err
	}

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

func executeDeleteTable() error {
	client, err := initTableClient()
	if err != nil {
		return err
	}

	in := &dynamodb.DeleteTableInput{TableName: aws.String(table)}
	out, err := client.DeleteTable(in)
	if err != nil {
		return err
	}
	writeVerbose(out)
	return nil
}

func executePutItem() error {
	client, err := initItemClient()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to initialize client: %v\n", err))
		return err
	}

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

func executeGetItem() error {
	client, err := initItemClient()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to initialize client: %v\n", err))
		return err
	}

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

func executeQuery() error {
	client, err := initItemClient()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to initialize client: %v\n", err))
		return err
	}

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

func executeScan() error {
	client, err := initItemClient()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to initialize client: %v\n", err))
		return err
	}

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

func writeVerbose(o interface{}) {
	if verbose != nil && *verbose {
		os.Stdout.WriteString(fmt.Sprintf("%v\n", o))
	}
}

func initTableClient() (tableClient, error) {
	if *service == "dax" {
		return nil, fmt.Errorf("for table operations use service 'dynamodb'")
	}
	return ddbClient(*region)
}

func initItemClient() (itemClient, error) {
	if *service == "dax" {
		return daxClient(*endpoint, *region)
	}
	return ddbClient(*region)
}

func ddbClient(region string) (*dynamodb.DynamoDB, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		return nil, err
	}
	return dynamodb.New(sess), nil
}

func daxClient(endpoint, region string) (itemClient, error) {
	cfg := dax.DefaultConfig()
	cfg.HostPorts = []string{endpoint}
	cfg.Region = region
	return dax.New(cfg)
}

func validate() error {
	if service == nil || !contains(*service, services) {
		return fmt.Errorf("service should be one of [%s]", strings.Join(services, " | "))
	}
	if _, ok := commandMap[*command]; !ok {
		return fmt.Errorf("command should be one of [%s]", commandsMsg)
	}
	if *service == "dax" {
		if endpoint == nil || len(*endpoint) == 0 {
			return fmt.Errorf("endpoint should be set for 'dax' service")
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

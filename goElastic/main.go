package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"io/ioutil"
	"net/http"
	"reflect"
)

var (
	client = http.Client{}
	url    = ""
)

type block struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Author          string   `json:"author"`
		Difficulty      string   `json:"difficulty"`
		ExtraData       string   `json:"extraData"`
		GasLimit        string   `json:"gasLimit"`
		GasUsed         string   `json:"gasUsed"`
		Hash            string   `json:"hash"`
		LogsBloom       string   `json:"logsBloom"`
		Miner           string   `json:"miner"`
		MixHash         string   `json:"mixHash"`
		Nonce           string   `json:"nonce"`
		Number          string   `json:"number"`
		ParentHash      string   `json:"parentHash"`
		ReceiptsRoot    string   `json:"receiptsRoot"`
		SealFields      []string `json:"sealFields"`
		Sha3Uncles      string   `json:"sha3Uncles"`
		Size            string   `json:"size"`
		StateRoot       string   `json:"stateRoot"`
		Timestamp       string   `json:"timestamp"`
		TotalDifficulty string   `json:"totalDifficulty"`
		Transactions    []struct {
			BlockHash        string      `json:"blockHash"`
			BlockNumber      string      `json:"blockNumber"`
			ChainID          interface{} `json:"chainId"`
			Condition        interface{} `json:"condition"`
			Creates          interface{} `json:"creates"`
			From             string      `json:"from"`
			Gas              string      `json:"gas"`
			GasPrice         string      `json:"gasPrice"`
			Hash             string      `json:"hash"`
			Input            string      `json:"input"`
			Nonce            string      `json:"nonce"`
			PublicKey        string      `json:"publicKey"`
			R                string      `json:"r"`
			Raw              string      `json:"raw"`
			S                string      `json:"s"`
			StandardV        string      `json:"standardV"`
			To               string      `json:"to"`
			TransactionIndex string      `json:"transactionIndex"`
			V                string      `json:"v"`
			Value            string      `json:"value"`
		} `json:"transactions"`
		TransactionsRoot string        `json:"transactionsRoot"`
		Uncles           []interface{} `json:"uncles"`
	} `json:"result"`
	ID int `json:"id"`
}

const blockMapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"block":{
			"properties":{
				
				"jsonrpc":{
					"type":"text" 
				},
				"id":{
					"type":"text"
				},
				"result":{
					"properties":{
						"author":{
							"type":"text"
						},
						"difficulty":{
							"type":"text"
						},
						"extraData":{
							"type":"text"
						},
						"gasLimit":{
							"type":"text"
						},
						"gasUsed":{
							"type":"text"
						},
						"hash":{
							"type":"text"
						},
						"logsBloom":{
							"type":"text"
						},
						"miner":{
							"type":"text"
						},
						"mixHash":{
							"type":"text"
						},
						"nonce":{
							"type":"text"
						},
						"number":{
							"type":"text"
						},
						"parentHash":{
							"type":"text"
						},
						"receiptsRoot":{
							"type":"text"
						},
						"sealFields":{
							"type":"text"
						},
						"sha3Uncles":{
							"type":"text"
						},
						"size":{
							"type":"text"
						},
						"stateRoot":{
							"type":"text"
						},
						"timestamp":{
							"type":"text"
						},
						"totalDifficulty":{
							"type":"text"
						},
						"transactions":{

							"properties":{
								"blockHash":{
									
									"type":"text"
								},
								"blockNumber":{
									
									"type":"text"
								},
								"chainId":{
									
									"type":"text"
								},
								"condition":{
									
									"type":"text"
								},
								"creates":{
									
									"type":"text"
								},
								"from":{
									
									"type":"text"
								},
								"gas":{
									
									"type":"text"
								},
								"gasPrice":{
									
									"type":"text"
								},
								"hash":{
									
									"type":"text"
								},
								"input":{
									
									"type":"text"
								},
								"nonce":{
									
									"type":"text"
								},
								"publicKey":{
									
									"type":"text"
								},
								"r":{
									
									"type":"text"
								},
								"raw":{
									
									"type":"text"
								},
								"s":{
									
									"type":"text"
								},
								"standardV":{
									
									"type":"text"
								},
								"to":{
									
									"type":"text"
								},
								"transactionIndex":{
									
									"type":"text"
								},
								"v":{
									
									"type":"text"
								},
								"value":{
									
									"type":"text"
								}

							}
						},
						"transactionsRoot":{
							"type":"text"
						},
						"uncles":{
							"type":"text"
						}
						
					}
				}
			}
		}
	}
}`

func convertToHex(blockNumber int) string {
	hexBytes := fmt.Sprintf("%0x", blockNumber)
	hexString := fmt.Sprintf("%s", hexBytes)
	hexString = "0x" + hexString
	return hexString
}
func getBlockData(blockNumber int) []byte {
	var body []byte
	blockNumberHex := convertToHex(blockNumber)

	var dataToSend = []byte(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["` + blockNumberHex + `", true],"id":1}`)

	fmt.Println("Fetching BlockNumber:", blockNumberHex)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(dataToSend))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Unable to reach the server.")
	} else {
		body, _ = ioutil.ReadAll(resp.Body)
		// fmt.Println("body=", string(body))

	}
	return body
}

func main() {

	ctx := context.Background()

	// Connect to ElasticSearch
	client, err := elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"))
	if err != nil {
		// Handle error
		panic(err)
	} else {
		fmt.Println("Connection to ElasticSearch Succesfull")
	}

	//Ping ElasticSearch
	info, code, err := client.Ping("http://127.0.0.1:9200").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists("block").Do(ctx)
	if !exists {
		fmt.Println("No Block Index Exists")
		// Create a new index.
		createIndex, err := client.CreateIndex("block").BodyString(blockMapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	} else {
		fmt.Println("Block Index already exists")
	}

	//get block data first
	var b block
	result := getBlockData(801)

	err = json.Unmarshal(result, &b)
	if err != nil {
		fmt.Println("ERROR")
	} else {
		// fmt.Println(b)
	}

	// Index a tweet (using JSON serialization)
	block1 := b
	put1, err := client.Index().
		Index("block").
		Type("block").
		Id("1").
		BodyJson(block1).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed block %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	// Search with a term query
	termQuery := elastic.NewTermQuery("id", "1")
	searchResult, err := client.Search().
		Index("block").   // search in index "twitter"
		Query(termQuery). // specify the query
		// Sort("user", true). // sort by "user" field, ascending
		From(0).Size(10). // take documents 0-9
		Pretty(true).     // pretty print request and response JSON
		Do(ctx)           // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	var bcd block
	for _, item := range searchResult.Each(reflect.TypeOf(bcd)) {
		if t, ok := item.(block); ok {
			fmt.Printf("Block  %s: %s\n", t.Result, t.Result.Hash)
		}
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())
}

package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"encoding/json"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.


	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		tasks, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Fatal(err)
		}

	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.

		dec := json.NewDecoder(strings.NewReader(tasks))
		for {
			var m Message
			if err := dec.Decode(&m); err == io.EOF {
				break
			}
			else if err != nil {
				log.Fatal(err)
			}
			fmt.Printf(m.Key, m.Value)
		}

		// split m into slices by looping through key, value pairs
		// loop through key, check if there is an existing slice
		// if so, append to that slice
		// if not, create a new slice and add key to array of existing slices

			for i := range(m) {
				slice := []*KeyValue{}
			}

		// reduceF() is the application's reduce function. You should
		// call it once per distinct key, with a slice of all the values
		// for that key. reduceF() returns the reduced value for that key.

		// You should write the reduce output as JSON encoded KeyValue
		// objects to the file named outFile. 

		// enc := json.NewEncoder(file)
		// for key := ... {
		// 	enc.Encode(KeyValue{key, reduceF(...)})
		// }
		// file.Close()

	}
}

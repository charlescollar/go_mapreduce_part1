package mapreduce

import ("fmt"; "sync")

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, 
				mapFiles []string, 
				nReduce int, 
				phase jobPhase, 
				registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map) 
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	//
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).

	//part B
	//change when you are calling call()
	//var test = go call(args)
	//if test = false -> Worker Failed, reassign task

	count:=0
	var myWaitGroup sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		worker:= <-registerChan
		count++
		myWaitGroup.Add(1) //increment waitgroup counter

		//task args struct 
		var myTaskStruct = DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		//concurrency on our RPC calls
		go func(){

			call(worker, "Worker.DoTask", myTaskStruct, nil)
			myWaitGroup.Done() //decrements waitGroup counter
			registerChan <- worker  //workers aren't getting past this on task 18 and 19
			count--
		}()
	}
	
	myWaitGroup.Wait()  //stops function from returning until waitgroup counter is at 0 (all tasks are done)
	fmt.Printf("Schedule: %v done\n", phase)
	
}

	


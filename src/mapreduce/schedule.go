package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
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


	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).

	
	//Read RegisterChan
	//create waitGroup


	//for each file in mapFiles, assign to worker (registerChanString) by sending RPC

	//send RPC to worker using call() function and DoTaskArgs struct
	//call(RegChanString, "Worker.DoTask", DoTasksArgs struct, nil)

	//our DoTaskArgs struct if(jobphase = map)
	//{jobName, mapFiles[i], jobphase, i, n_other}
	//if(jobphase = reduce) --> leave out the mapFiles[i] argument

	//waitgroup.add(1)  -->increment the waitgroup counter


	//when calling call(), need to use concurrency! 
	//go call(args)
	//use Scanln() func so program doesn't exit early

	//when worker completes its task, waitgroup.done()  -->decrements waitgroup counter
	//QUESTION: how to tell when worker completes task? channels? 

	//at the end of func, waitgroup.Wait()  -->waits to return the function until waitgroup counter is at 0 
	//ie until all tasks are complete


	//part B
	//change when you are calling call()
	//var test = go call(args)
	//if test = false -> Worker Failed, reassign task


	fmt.Printf("Schedule: %v done\n", phase)
}

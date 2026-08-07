package main

import (
	"fmt"
	"taskq/internal/queue"
)

func main() {
	var q queue.Queue

	tasks := []queue.Task{
		{
			Type:    "email",
			Payload: map[string]any{"userID": 007},
		},
		{
			Type:    "image",
			Payload: map[string]any{"imageID": 007},
		},
		{
			Type:    "report",
			Payload: map[string]any{"reportID": 007},
		},
	}

	fmt.Println("Producer:")
	for i, task := range tasks {
		q.Enqueue(task)
		fmt.Printf("Enqueued task #%d (%s) \n", i+1, task.Type)
	}

	fmt.Println("\nWorker:")

	for {
		task, ok := q.Dequeue()
		if !ok {
			fmt.Println("\nQueue Empty")
			break
		}
		fmt.Printf("Dequeued task %d (%s) \n", task.ID, task.Type)
		fmt.Printf("Processing %s task ...\n", task.Type)
		fmt.Println("Done.")
	}

}

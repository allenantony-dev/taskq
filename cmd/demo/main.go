package main

import (
	"fmt"
	"taskq/internal/queue"
	"taskq/internal/worker"
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
	for _, task := range tasks {
		id := q.Enqueue(task)
		fmt.Printf("Enqueued task #%d (%s) \n", id, task.Type)
	}

	fmt.Println("\nWorker:")

	handlers := map[string]worker.Handler{
		"email": func(task queue.Task) error {
			fmt.Println("Sending email ...")
			return nil
		},
		"image": func(task queue.Task) error {
			fmt.Println("Resizing image ...")
			return nil
		},
		"report": func(task queue.Task) error {
			fmt.Println("Generating report ...")
			return nil
		},
	}

	w := worker.NewWorker(&q, handlers)
	w.Run()

}

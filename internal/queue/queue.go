package queue

type Queue struct {
	nextID int
	tasks  []Task
}

func (q *Queue) Enqueue(task Task) {
	q.nextID++
	task.ID = q.nextID
	q.tasks = append(q.tasks, task)
}

func (q *Queue) Dequeue() (Task, bool) {
	if len(q.tasks) == 0 {
		return Task{}, false
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]

	return task, true
}

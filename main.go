package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Task struct {
	id int
	data string
	timeLeft int
	timeTaken int
	numOfFail int
	retryLimit int
	numOfTimesProcessed int
}

type Worker struct {
	id int
	quantum int
	tasks []Task
}

func (w *Worker) addToTasks (t Task) {
	exists := false
	for _, task := range w.tasks {
		if task.id == t.id {
			exists = true
			break
		}
	}

	if !exists {
		w.tasks = append(w.tasks, t)
	}
}

func (w Worker) execut (t Task, m *Manager, wg *sync.WaitGroup) {
	t.numOfTimesProcessed += 1
	fmt.Printf("Worker %d is working on Task %d: %s proceesed %d times\n", w.id, t.id, t.data, t.numOfTimesProcessed)

	if rand.Float32() < 0.3 {
		t.numOfFail += 1
		if t.numOfFail < t.retryLimit {
			fmt.Printf("Worker %d failed on taks %d: %s\n", w.id, t.id, t.data)
			fmt.Printf("Task %d re-enters the system", t.id)
			m.loadTask(t)
		} else {
			fmt.Printf("Task %d: %s Was a faliure processed %d times, failed %d times\n", t.id, t.data, t.numOfTimesProcessed, t.numOfFail)
			m.numOfFailedTasks++
			m.checkAndClose()
		}
	} else if t.timeLeft >= w.quantum {
		time.Sleep(time.Duration(w.quantum)*time.Second)
		t.timeLeft--
		m.loadTask(t)
	} else {
		fmt.Printf("Task %d: %s Was a success processed %d times, failed %d times\n", t.id, t.data, t.numOfTimesProcessed, t.numOfFail)
		m.numOfCompletedTasks++
		m.checkAndClose()
	}

	wg.Done()
}

type Manager struct {
	isDone bool
	tasks []Task
	numOfTasks int
	workers []Worker
	numOfWorkers int
	wg *sync.WaitGroup
	taskChan chan Task
	numOfFailedTasks int
	numOfCompletedTasks int
}

func (m Manager) loadTask (t Task) {
	m.taskChan <- t
}

func (m Manager) loadAllTasks () {
	for _, task := range m.tasks {
		m.loadTask(task)
	}
}

func (m *Manager) checkAndClose () {
	if (m.numOfCompletedTasks + m.numOfFailedTasks >= m.numOfTasks) && !m.isDone {
		close(m.taskChan)
		m.isDone = true
	}
}

func (m *Manager) assign () {
	for task := range m.taskChan {
		i := rand.Intn(m.numOfWorkers)
		m.workers[i].addToTasks(task)
		m.wg.Add(1)
		go m.workers[i].execut(task, m, m.wg)
	}
}

func (m *Manager) run () {
	m.loadAllTasks()
	m.assign()
}

func createTask (id int, data string, retryLimit int) Task {
	timeTaken := rand.Intn(5) + 3
	return Task {
		id: id, 
		data: data, 
		timeTaken: timeTaken, 
		timeLeft: timeTaken, 
		retryLimit: retryLimit,
	}
}

func createManager (tasks []Task, workers []Worker, wg *sync.WaitGroup) Manager {
	return Manager {
		isDone: false,
		tasks: tasks,
		wg: wg,
		numOfTasks: len(tasks),
		workers: workers,
		numOfWorkers: len(workers),
		taskChan: make(chan Task, len(tasks)),
		numOfFailedTasks: 0,
		numOfCompletedTasks: 0,
	}
}

func main () {
	const retryLimit = 3
	var wg sync.WaitGroup
	
	tasks := []Task {
		createTask(1, "Estif's Task", retryLimit),
		createTask(2, "Fev's Task", retryLimit),
		createTask(3, "Yos's Task", retryLimit),
		createTask(4, "Mom's Task", retryLimit),
		createTask(5, "Dad's Task", retryLimit),
	}
	workers := []Worker {
		{id: 1, quantum: 1},
		{id: 2, quantum: 1},
		{id: 3, quantum: 1},
	}
	taskManager := createManager(tasks, workers, &wg)
	taskManager.run()

	taskManager.wg.Wait()
	fmt.Println("End Program")
	fmt.Println()
	fmt.Println("Report")
	for _, worker := range taskManager.workers {
		fmt.Printf("Worker %d contributed to tasks:\n", worker.id)
		for _, task := range worker.tasks {
			fmt.Println("	" + task.data)
		}
	}
	fmt.Println()
	fmt.Printf("Number of Failed Tasks: %d\n", taskManager.numOfFailedTasks)
	fmt.Printf("Number of Completed Tasks: %d\n", taskManager.numOfCompletedTasks) 
}
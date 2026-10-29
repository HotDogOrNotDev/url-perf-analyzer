package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"text/tabwriter"
	"time"
)

const (
	workerCount = 5
	statusDone  = "обработан"

	minDelay = 40 * time.Millisecond
	maxDelay = 200 * time.Millisecond
)

// Job - одно задание на обработку URL
type Job struct {
	ID  int
	URL string
}

// Result - итог обработки одного задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

func main() {
	urls := []string{
		"https://golang.org",
		"https://go.dev",
		"https://pkg.go.dev",
		"https://example.com",
		"https://example.org",
		"https://httpbin.org/get",
		"https://jsonplaceholder.typicode.com/posts/1",
		"https://api.github.com",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://news.ycombinator.com",
		"https://stackoverflow.com/questions",
		"https://developer.mozilla.org",
	}

	// Буфер равен числу заданий: main сначала кладёт все Job и только потом
	// читает Result. Без буфера воркеры заблокируются на отправке результата,
	// а main — на отправке следующего задания.
	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	started := time.Now()
	var wg sync.WaitGroup

	// Fan-out: фиксированный пул воркеров читает общий канал заданий.
	for range workerCount {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// Fan-in: воркеры пишут в один канал. Закрываем его только после того,
	// как все воркеры завершились, иначе range в main зависнет или пропустит данные.
	go func() {
		wg.Wait()
		close(results)
	}()

	for i, url := range urls {
		jobs <- Job{ID: i + 1, URL: url}
	}
	close(jobs)

	collected := make([]Result, 0, len(urls))
	for result := range results {
		collected = append(collected, result)
	}
	elapsed := time.Since(started)

	if len(collected) != len(urls) {
		fmt.Fprintf(os.Stderr, "обработано %d из %d URL\n", len(collected), len(urls))
		os.Exit(1)
	}

	printReport(collected, elapsed)
}

// worker читает задания до закрытия канала и отправляет результат каждого.
func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		start := time.Now()
		simulateHTTPRequest()
		results <- Result{
			Job:      job,
			Status:   statusDone,
			Duration: time.Since(start),
		}
	}
}

// simulateHTTPRequest имитирует сетевой запрос случайной задержкой.
func simulateHTTPRequest() {
	span := int64(maxDelay - minDelay)
	delay := minDelay + time.Duration(rand.Int64N(span))
	time.Sleep(delay)
}

func printReport(results []Result, elapsed time.Duration) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].Job.ID < results[j].Job.ID
	})

	var total time.Duration
	success := 0
	minDuration := results[0].Duration
	maxDuration := results[0].Duration

	fmt.Println("Отчёт по анализу производительности URL")
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tURL\tСтатус\tВремя")
	fmt.Fprintln(w, "--\t---\t------\t-----")
	for _, result := range results {
		total += result.Duration
		if result.Status == statusDone {
			success++
		}
		if result.Duration < minDuration {
			minDuration = result.Duration
		}
		if result.Duration > maxDuration {
			maxDuration = result.Duration
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
			result.Job.ID,
			result.Job.URL,
			result.Status,
			result.Duration.Round(time.Millisecond),
		)
	}
	w.Flush()

	average := total / time.Duration(len(results))

	fmt.Println()
	fmt.Printf("Воркеров в пуле:     %d\n", workerCount)
	fmt.Printf("Всего URL:           %d\n", len(results))
	fmt.Printf("Успешно обработано:  %d\n", success)
	fmt.Printf("Сумма длительностей: %s\n", total.Round(time.Millisecond))
	fmt.Printf("Среднее время:       %s\n", average.Round(time.Millisecond))
	fmt.Printf("Минимум:             %s\n", minDuration.Round(time.Millisecond))
	fmt.Printf("Максимум:            %s\n", maxDuration.Round(time.Millisecond))
	fmt.Printf("Время конвейера:     %s\n", elapsed.Round(time.Millisecond))
}

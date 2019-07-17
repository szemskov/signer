package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

func debug(input string, output string, operation string) {
	pc, _, _, _ := runtime.Caller(1)
	fmt.Printf("%s %s %s %s\n", input, runtime.FuncForPC(pc).Name(), operation, output)
}

/**
SingleHash считает значение crc32(data)+"~"+crc32(md5(data)) ( конкатенация двух строк через ~),
где data - то что пришло на вход (по сути - числа из первой функции)
*/
func SingleHash(in, out chan interface{}) {
	fmt.Println("SingleHash start...")
	mutex := &sync.Mutex{}
	group := sync.WaitGroup{}

	for val := range in {
		data := fmt.Sprint(val.(int))

		crc32Md5 := make(chan string, 1)
		crc32 := make(chan string, 1)

		group.Add(1)
		go func(crc32Md5 chan string, mutex *sync.Mutex) {
			defer group.Done()
			defer close(crc32Md5)

			// calc md5
			mutex.Lock()
			md5Data := DataSignerMd5(data)
			debug(data, md5Data, "md5(data)")
			mutex.Unlock()
			runtime.Gosched()

			// calc crc32
			crc32Md5Data := DataSignerCrc32(md5Data)
			crc32Md5 <- crc32Md5Data

		}(crc32Md5, mutex)
		runtime.Gosched()

		group.Add(1)
		go func(crc32 chan string) {
			defer group.Done()
			defer close(crc32)

			crc32Data := DataSignerCrc32(data)
			crc32 <- crc32Data
		}(crc32)
		runtime.Gosched()

		group.Add(1)
		go func(crc32 chan string, crc32Md5 chan string) {
			defer group.Done()
			crc32Data := <-crc32
			debug(data, crc32Data, "crc32(data)")

			crc32Md5Data := <-crc32Md5
			debug(data, crc32Md5Data, "crc32(md5(data))")

			result := crc32Data + "~" + crc32Md5Data
			out <- result

			debug(data, result, "result")
		}(crc32, crc32Md5)
		runtime.Gosched()
	}

	group.Wait()

	fmt.Println("SingleHash finished!")
}

/**
 MultiHash считает значение crc32(th+data)) (конкатенация цифры, приведённой к строке и строки),
 где th=0..5 ( т.е. 6 хешей на каждое входящее значение ),
 потом берёт конкатенацию результатов в порядке расчета (0..5),
где data - то что пришло на вход (и ушло на выход из SingleHash)
*/
func MultiHash(in, out chan interface{}) {
	fmt.Println("MultiHash start...")
	wg1 := &sync.WaitGroup{}

	for dataRaw := range in {
		data, ok := dataRaw.(string)
		if !ok {
			panic("MultiHash:: cant convert result data to string")
		}

		workerChannel := make(chan int, 1)

		wg1.Add(1)
		go func(wg *sync.WaitGroup, workerChannel chan int, data string) {
			defer wg.Done()

			mutex := &sync.Mutex{}
			wg2 := &sync.WaitGroup{}
			hashes := make([]string, 6, 6)

			for num := range workerChannel {
				wg2.Add(1)
				go func(wg *sync.WaitGroup, mutex *sync.Mutex, num int) {
					defer wg.Done()

					crc32Data := DataSignerCrc32(fmt.Sprint(num) + data)
					debug(data, crc32Data, fmt.Sprintf("crc32(th+step1)) %d", num))

					mutex.Lock()
					hashes[num] = crc32Data
					mutex.Unlock()

				}(wg2, mutex, num)
				runtime.Gosched()
			}

			wg2.Wait()
			result := strings.Join(hashes, "")
			debug(data, result, "result")
			out <- result
		}(wg1, workerChannel, data)
		runtime.Gosched()

		for _, num := range []int{0, 1, 2, 3, 4, 5} {
			workerChannel <- num
		}
		close(workerChannel)
	}

	wg1.Wait()

	fmt.Println("MultiHash finished!")
}

/**
CombineResults получает все результаты, сортирует (https://golang.org/pkg/sort/),
объединяет отсортированный результат через _ (символ подчеркивания) в одну строку
*/
func CombineResults(in, out chan interface{}) {
	fmt.Println("CombineResults start...")
	result := make([]string, 0, 10)

	for dataRaw := range in {
		data := dataRaw.(string)
		result = append(result, data)
	}
	debug("", strings.Join(result, "_"), "combine")
	sort.Strings(result)

	fmt.Println("CombineResults finished!")
	out <- strings.Join(result, "_")
}

func startWorker(group *sync.WaitGroup, worker job, workerInput chan interface{}, workerOutput chan interface{}) {
	defer group.Done()
	defer close(workerOutput)
	worker(workerInput, workerOutput)
}

func ExecutePipeline(jobs ...job) {
	group := &sync.WaitGroup{}

	workerInput := make(chan interface{}, 1)
	for _, worker := range jobs {
		workerOutput := make(chan interface{}, 1)
		group.Add(1)
		go startWorker(group, worker, workerInput, workerOutput)
		workerInput = workerOutput
	}
	time.Sleep(time.Millisecond * 100)
	group.Wait()
}

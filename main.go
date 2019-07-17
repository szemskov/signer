package main

import "fmt"

func main() {
	inputData := []int{0, 1}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			for dataRaw := range in {
				data, ok := dataRaw.(string)
				if !ok {
					panic("Main:: cant convert result data to string")
				}
				fmt.Println("Finish", data)
			}
		}),
	}

	ExecutePipeline(hashSignJobs...)

}

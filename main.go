package main

import (
	"log"

	"github.com/pranav1698/data-ingest-pipeline/pipeline"
)


func main() {
	log.Print("Starting Application....")

	fileName := "/home/pranav/go/src/data-ingest-pipeline/files/https___www.thisisbarry.com_-Top target pages-2022-08-01.csv"

	var pipeline pipeline.IPipeline = &pipeline.Pipeline{}
	pipe, err := pipeline.NewPipeline()
	if err != nil {
		log.Println("Error: ", err)
		return
	}

	err = pipe.ProcessFile(fileName)
	if err != nil {
		log.Println("Error: ", err)
		return
	}
}




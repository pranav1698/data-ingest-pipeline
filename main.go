package main

import (
	"os"
	"log"
	"errors"
	"fmt"

	"github.com/pranav1698/data-ingest-pipeline/fileUtil"
	"github.com/pranav1698/data-ingest-pipeline/database"
	"github.com/pranav1698/data-ingest-pipeline/table"
)


func main() {
	log.Print("Starting Application....")

	fileName := "/home/pranav/go/src/data-ingest-pipeline/files/https___www.thisisbarry.com_-Top target pages-2022-08-01.csv"
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("Error opening file: %s", err)
		return
	}
	defer file.Close()

	err = CheckFile(fileName)
	if err != nil {
		log.Println("Error: ", err)
		return 
	}

	date := GetDateFromFileName(fileName)
	log.Println(date)

	db, err := database.GetDatabaseConnection()
	if err != nil {
		log.Println("Error: ", err)
		return 
	}

	var metricRecord database.IMetricRecord = &database.MetricRecord{}
	err = metricRecord.CreateMetricsTable(db)
	if err != nil {
		log.Println(err)
		return
	}

	var siteRecord database.ISiteRecord = &database.SiteRecord{}
	err = siteRecord.CreateSiteTable(db)
	if err != nil {
		log.Println("Error: ", err)
		return 
	}

	var csvData table.ICSVData = &table.CSVData{}
	data, err := csvData.GetCSVData(fileName)
	if err != nil {
		log.Println("Error: ", err)
		return
	}
	
	headers, err := csvData.GetCSVHeaders(fileName)
	if err != nil {
		log.Println("Error: ", err)
		return
	}
	
	columns, err := metricRecord.GetColumnsOfMetricsTable(db)
	if err != nil {
		log.Println("Error: ", err)
		return
	}

	err = CheckColumnsInDatabase(headers, columns)
	if err != nil {
		log.Println("Error: ", err)
		return
	}

	var iSiteRecord database.ISiteRecord = &database.SiteRecord{};
	var iMetricRecord database.IMetricRecord = &database.MetricRecord{};
	for _, record := range data {
		siteRecord := iSiteRecord.NewSiteRecord(record.TargetPage)
		site, err := iSiteRecord.InsertRecordInSitesTable(db, siteRecord)
		if err != nil {
			log.Println("Error: ", err)
			return
		}

		metricRecord := iMetricRecord.NewMetricRecord(site.TargetPageId, "2022-08-01", record.IncomingLinks, record.LinkingSites)
		_, err = iMetricRecord.InsertRecordInMetricsTable(db, metricRecord)
		if err != nil {
			log.Println("Error: ", err)
			return
		}
	}
}

func CheckFile(fileName string) (error) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	isExcel := util.CheckExtension(fileName)
	if !isExcel {
		err := errors.New("Not a Excel File, please provide an excel or csv file as input")
		return err
	}

	checkFormat := util.CheckFormat(fileName)
	if !checkFormat {
		err := errors.New("Please check that file adheres to predefined format, for e.g.: https___www.thisisbarry.com_-Top target pages-2022-08-01.csv")
		return err
	}

	return nil
}

func GetDateFromFileName(fileName string) (string) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	
	date := util.GetDate(fileName)
	return date
}

func CheckColumnsInDatabase(headers []string, columns []string) (error) {
	for _, columnHeader := range headers {
		if columnHeader == "Target page" {
			continue
		}
		flag := false

		for _, dbColumnHeader := range columns {
			if dbColumnHeader == columnHeader {
				flag = true
			}
		}
		
		if !flag {
			err := fmt.Errorf("%s not present in database.", columnHeader)
			return err
		}
		
	}

	return nil
}


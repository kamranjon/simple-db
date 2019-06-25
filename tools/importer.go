package tools

import(
  "encoding/csv"
  "fmt"
  "os"
  "log"
  "strings"
  "strconv"
  "time"
  "github.com/kamranjon/simple-db/db"
  "github.com/kamranjon/simple-db/models"
)

type Importer struct {
  File string
  Db db.Db
}

func parseCSV(file string) ([][]string, error) {
  csvFile, err := os.Open(file)
  if err != nil {
    log.Println("Failed to open CSV file for importing", err)
    return [][]string{}, err
  }

  defer csvFile.Close()

  reader := csv.NewReader(csvFile)

  reader.Comma = '|'

  reader.FieldsPerRecord = -1

  csvData, err := reader.ReadAll()
  if err != nil {
    log.Println(err)
    os.Exit(1)
  }

  return csvData, nil
}

func convertToUnixTime(time_string string) (int64, error) {
  date_layout := "2006-01-02"
  date, err := time.Parse(date_layout, time_string)
  if err != nil {
    log.Println("Failed to convert date field", err)
    return 0, err
  }
  return date.Unix(), nil
}

func parseDuration(duration string) (int32, error) {
  durationString := strings.Replace(duration, ":", "h", 1)
  durationString = fmt.Sprintf("%sm", durationString)

  viewTime, err := time.ParseDuration(durationString)

  if err != nil {
    log.Println("Failed to parse duration", err)
    return 0, err
  }
  return int32(viewTime.Minutes()), nil
}

func (importer Importer) ImportCSV() error {

	csvData, err := parseCSV(importer.File)
	if err != nil {
		return err
	}

  for i, row := range csvData {
    // skip header row
    if (i == 0) {
      continue
    }

    // convert time format to unix timestamp
    date, err := convertToUnixTime(row[3])
    if err != nil {
      return err
    }

    // parse float for rev
    rev, err := strconv.ParseFloat(row[4], 32)
    if err != nil {
      log.Println("Failed to parse rev field", err)
      return err
    }


    // parse duration out of colon time format
    viewTime, err := parseDuration(row[5])
    if err != nil {
      return err
    }

    viewEntry := models.ViewEntry{
      Stb: row[0],
      Title: row[1],
      Provider: row[2],
      Rev: rev,
      Date: date,
      ViewTime: viewTime,
    }

    importer.Db.Put(viewEntry)
  }
  return nil
}
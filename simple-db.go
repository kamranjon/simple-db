package main

import (
  "fmt"
  "log"
  "github.com/kamranjon/simple-db/db"
  "github.com/kamranjon/simple-db/tools"
  "github.com/kamranjon/simple-db/query"

  "github.com/spf13/cobra"
)

func main() {
	var selectColumns []string
	var orderColumns []string
	var filterClauses map[string]string

  var cmdQuery = &cobra.Command{
    Use:   "query",
    Short: "query with select, order and filter",
    Run: func(cmd *cobra.Command, args []string) {
			active_db, err := db.OpenParquetDb()
			defer active_db.Index.Db.Close()
			if err != nil {
				log.Println("Failed to create new DB:", err)
				return
			}

    	f := make(map[string]interface{})
    	log.Println(orderColumns, filterClauses, args)
	    for k, v := range filterClauses {
	      f[k] = v
	    }
    	query := query.NewQuery().Filter(f).Order(orderColumns...)
    	results := active_db.Query(query)
      fmt.Println("Results: ", results)
    },
  }

  var cmdImport = &cobra.Command{
    Use:   "import",
    Short: "Imports data from the assets directory",
    Run: func(cmd *cobra.Command, args []string) {
			active_db, err := db.OpenParquetDb()
			defer active_db.Index.Db.Close()
			if err != nil {
				log.Println("Failed to create new DB:", err)
				return
			}

			importer := tools.Importer{
				File: "assets/data_sample.psv",
				Db: active_db,
			}

      importer.ImportCSV()
      active_db.WriteOut()
    },
  }

  cmdQuery.Flags().StringSliceVarP(&selectColumns, "select", "s", []string{}, "columns to select")
	cmdQuery.Flags().StringSliceVarP(&orderColumns, "order", "o", []string{}, "columns to order")
	cmdQuery.Flags().StringToStringVarP(&filterClauses, "filter", "f", map[string]string{}, "key=value filers")

	var rootCmd = &cobra.Command{Use: "app"}
	rootCmd.AddCommand(cmdQuery, cmdImport)
	rootCmd.Execute()

}

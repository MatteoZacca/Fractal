package main

import (
	"log"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all files currently stored in the Fractal cluster",
	Run: func(cmd *cobra.Command, args []string) {
		err := client.ListFiles()
		if err != nil {
			log.Fatalf("[list] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}

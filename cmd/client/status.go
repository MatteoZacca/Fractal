package main

import (
	"log"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Displays the current status of the Fractal cluster",
	Run: func(cmd *cobra.Command, args []string) {
		err := client.GetClusterStatus()
		if err != nil {
			log.Fatalf("[status] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

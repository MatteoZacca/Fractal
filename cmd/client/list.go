package main

import (
	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all files currently stored in the Fractal cluster",
	Run: func(cmd *cobra.Command, args []string) {
		client.ListFiles()
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}

package main

import (
	"log"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read [path\\to\\file\\in\\docker]",
	Short: "Download and reassemble a file from the Fractal cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]

		err := client.DownloadFile(fileName)
		if err != nil {
			log.Fatalf("[read] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}

package main

import (
	"log"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var burnCmd = &cobra.Command{
	Use:   "burn [path\\to\\file\\in\\docker]",
	Short: "Permanently delete a file form the cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]

		err := client.DeleteFile(fileName)
		if err != nil {
			log.Fatalf("[burn] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(burnCmd)
}

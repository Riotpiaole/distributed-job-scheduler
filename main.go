// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {

	// // Run the pipeline. You can specify your runner with the --runner flag.
	// if err := beamx.Run(ctx, pipeline); err != nil {
	// 	log.Fatalf("Failed to execute job: %v", err)
	// }

	var rootCmd = &cobra.Command{
		Use:   "slogger [inputs...] processor.so",
		Short: "Run an ETL process with given files and a shared object plugin",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			// The last argument is the .so file
			processor := args[len(args)-1]

			// Everything before the last argument is part of the input
			inputs := args[:len(args)-1]

			fmt.Printf("🚀 Starting ETL Process\n")
			fmt.Printf("📂 Inputs:    %v\n", inputs)
			fmt.Printf("⚙️  Processor: %s\n", processor)

			// Logic to handle DB URL vs File Dir vs File List would go here
			executeETL(inputs, processor)
		},
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

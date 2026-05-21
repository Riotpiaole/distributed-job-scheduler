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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unicode"

	"github.com/joho/godotenv"
	"github.com/pbnjay/memory"
	"github.com/spf13/cobra"
	"riotpiaole.com/vec_db_pipeline/pipeline"
	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

func defaultOutputDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return pipeline.DefaultOutputDir
	}
	return filepath.Join(wd, pipeline.DefaultOutputDir)
}

func main() {
	var outputDir string

	var rootCmd = &cobra.Command{
		Use:   " [inputs...] processor.so",
		Short: "Run an ETL process with given files and a shared object plugin",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			inputs := args[0]

			fmt.Printf("🚀 Starting ETL Process\n")
			fmt.Printf("📂 Inputs:    %v\n", inputs)
			fmt.Printf("📁 Output:    %v\n", outputDir)

			RunPipeline(inputs, outputDir)
		},
	}

	// var workerCmd = &cobra.Command{
	// 	Use:   "worker",
	// 	Short: "Run a worker",
	// 	Args:  cobra.MinimumNArgs(3),
	// 	Run: func(cmd *cobra.Command, args []string) {
	// 		// _ := args[0]
	// 		pipeline.StartWorker(uuid.New().String(), []pipeline.StreamProcessAction{}, outputDir)
	// 	},
	// }
	// rootCmd.AddCommand(workerCmd)
	rootCmd.Flags().StringVarP(&outputDir, "output", "o", defaultOutputDir(), "directory for intermediate and output files")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// resolveNumWorkers determines how many goroutine workers to launch.
//
// Resolution order:
//  1. NUM_WORKER in .env / process environment — user-supplied hard limit.
//  2. Memory-bound fallback — derived from free RAM divided by the per-goroutine
//     stack budget (4 KB).  If the result is still zero (extremely constrained
//     host), NumCPU is used as a last-resort floor.
func resolveNumWorkers() int {
	// Each goroutine starts with a 4 KB stack; use that as the budget unit.
	const bytesPerWorker = 4 * 1024

	// L1: load .env into the process environment (no-op when file is absent).
	_ = godotenv.Load()

	// L1: user-specified value takes priority over any automatic calculation.
	if raw := os.Getenv("NUM_WORKER"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			fmt.Printf("NUM_WORKER from env: %d\n", n)
			return n
		}
		// Value is present but malformed — warn and fall through to the resource-based path.
		fmt.Fprintf(os.Stderr, "invalid NUM_WORKER=%q, falling back to memory-bound calculation\n", raw)
	}

	// Fallback: compute from available RAM so we never over-commit goroutines.
	free := memory.FreeMemory()
	NUM_WORKER := int(free / bytesPerWorker)

	// Guard against NUM_WORKER == 0 on a heavily loaded or very small host.
	if NUM_WORKER < 1 {
		NUM_WORKER = runtime.NumCPU()
	}

	fmt.Printf("NUM_WORKER (memory-bound): %d  (free RAM: %d MB)\n", NUM_WORKER, free/1024/1024)
	return NUM_WORKER
}

func RunPipeline(filePath string, outputDir string) {
	numWorkers := 10
	datasource := datasource.FilesDataSource{
		FilePath: filePath,
	}
	ppl := pipeline.NewPipeline(&datasource, numWorkers)
	ppl.OutputDir = outputDir
	ppl.Map(func(args ...any) any {
		filename, _ := args[0].(string)
		contents, _ := args[1].(string)
		_ = filename

		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(contents, ff)

		kva := make([]pipeline.KeyValue, 0, len(words))
		for _, w := range words {
			kva = append(kva, pipeline.KeyValue{Key: pipeline.Key(w), Value: "1"})
		}
		return kva
	})

	ppl.Reduce(func(args ...any) any {
		values, _ := args[1].([]string)
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	})
	ppl.Start()
}

// Copyright (c) 2025 seqyuan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seqyuan/annogene/io/fastq"
)

const (
	Version   = "0.1.2"
	BuildTime = "2025-10-27"
)

// 默认的替换配置
const (
	DefaultFromPrefix = "ML15"
	DefaultToPrefix   = "E251"
)

// 简化的结果结构 - 只需要单端reads信息
type ProcessResult struct {
	Seq fastq.Sequence
}

// 带序号的批次结果 - 用于保持顺序
type OrderedBatch struct {
	Batch    []*ProcessResult
	Sequence int64 // 批次序号
}

// 修改readID中的指定前缀
func modifyReadID(readID, fromPrefix, toPrefix string) string {
	fromPattern := "@" + fromPrefix
	if strings.HasPrefix(readID, fromPattern) {
		return "@" + toPrefix + readID[len(fromPattern):]
	}
	return readID
}

// 获取文件basename（保持原始扩展名，但移除.gz）
func getBasename(filePath string) string {
	filename := filepath.Base(filePath)

	// 移除.gz扩展名
	if strings.HasSuffix(filename, ".gz") {
		filename = strings.TrimSuffix(filename, ".gz")
	}

	return filename
}

// 创建reader，支持普通文件和gz文件
func createReader(filePath string) (io.Reader, *os.File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}

	var reader io.Reader = file

	// 检查是否为gz文件
	if strings.HasSuffix(filePath, ".gz") {
		// 使用gzip reader，增加缓冲区大小
		gz, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to create gzip reader for %s: %v", filePath, err)
		}
		// 使用更大的缓冲区包装gzip reader
		reader = bufio.NewReaderSize(gz, 1024*1024) // 1MB缓冲区
	}

	return reader, file, nil
}

// 创建writer - 只输出文本文件，不压缩
func createWriter(filePath string) (io.Writer, *os.File, error) {
	// 创建输出目录
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create output file %s: %v", filePath, err)
	}

	return file, file, nil
}

// 处理单个read的worker函数 - 修改readID
func processRead(seq fastq.Sequence, fromPrefix, toPrefix string) *ProcessResult {
	// 修改readID
	modifiedSeq := fastq.Sequence{
		ID1:     []byte(modifyReadID(string(seq.ID1), fromPrefix, toPrefix)),
		Letters: seq.Letters,
		ID2:     seq.ID2,
		Quality: seq.Quality,
	}

	return &ProcessResult{
		Seq: modifiedSeq,
	}
}

// 批量处理reads的worker函数
func processReadBatch(reads []fastq.Sequence, fromPrefix, toPrefix string) []*ProcessResult {
	results := make([]*ProcessResult, 0, len(reads))

	for _, read := range reads {
		result := processRead(read, fromPrefix, toPrefix)
		results = append(results, result)
	}

	return results
}

// 使用pigz压缩文件
func compressWithPigz(file, pigzPath string, numWorkers int) error {
	// 检查pigz路径是否存在
	if _, err := os.Stat(pigzPath); os.IsNotExist(err) {
		return fmt.Errorf("pigz not found at path: %s", pigzPath)
	}

	// 压缩文件
	return compressFileWithPigz(file, pigzPath, numWorkers)
}

// 使用pigz解压缩gzip文件
func decompressWithPigz(filePath, pigzPath string, numWorkers int) (string, error) {
	// 检查pigz路径是否存在
	if _, err := os.Stat(pigzPath); os.IsNotExist(err) {
		return filePath, fmt.Errorf("pigz not found at path: %s", pigzPath)
	}

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "ml2es_*.fq")
	if err != nil {
		return filePath, fmt.Errorf("failed to create temp file: %v", err)
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()

	// 构建pigz命令
	cmd := exec.Command(pigzPath, "-d", "-p", fmt.Sprintf("%d", numWorkers), "-o", tempFilePath, filePath)

	// 设置输出
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// 执行解压缩
	if err := cmd.Run(); err != nil {
		os.Remove(tempFilePath) // 清理临时文件
		return filePath, fmt.Errorf("failed to decompress %s: %v", filePath, err)
	}

	fmt.Printf("Decompressed %s to %s using pigz\n", filePath, tempFilePath)
	return tempFilePath, nil
}

// 使用pigz压缩单个文件
func compressFileWithPigz(filePath, pigzPath string, numWorkers int) error {
	// 构建pigz命令
	cmd := exec.Command(pigzPath, "-p", fmt.Sprintf("%d", numWorkers), filePath)

	// 设置输出
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// 执行命令
	return cmd.Run()
}

// 高性能缓冲写入器 - 优化锁粒度
type BufferedFastqWriter struct {
	mu      sync.Mutex
	buffer  []byte
	writer  fastq.Writer
	file    *os.File
	maxSize int
	written int64 // 使用int64支持原子操作
}

func NewBufferedFastqWriter(writer fastq.Writer, file *os.File, bufferSize int) *BufferedFastqWriter {
	return &BufferedFastqWriter{
		buffer:  make([]byte, 0, bufferSize),
		writer:  writer,
		file:    file,
		maxSize: bufferSize,
	}
}

func (bfw *BufferedFastqWriter) Write(seq fastq.Sequence) error {
	// 使用原子操作更新统计，减少锁竞争
	written := atomic.AddInt64(&bfw.written, 1)

	// 只在需要刷新时获取锁
	if written%100000 == 0 {
		bfw.mu.Lock()
		defer bfw.mu.Unlock()

		// 直接使用fastq.Writer，它已经处理了序列化
		if _, err := bfw.writer.Write(seq); err != nil {
			return err
		}

		return bfw.flush()
	}

	// 大部分情况下不需要锁，直接写入
	if _, err := bfw.writer.Write(seq); err != nil {
		return err
	}

	return nil
}

func (bfw *BufferedFastqWriter) flush() error {
	// 强制同步到磁盘
	return bfw.file.Sync()
}

func (bfw *BufferedFastqWriter) Flush() error {
	bfw.mu.Lock()
	defer bfw.mu.Unlock()
	return bfw.flush()
}

func (bfw *BufferedFastqWriter) Close() error {
	if err := bfw.Flush(); err != nil {
		return err
	}
	return bfw.file.Close()
}

func usage() {
	fmt.Printf("\nProgram: ml2es - FastQ read ID modifier (configurable prefix replacement)\n")
	fmt.Printf("Usage: ml2es [options]\n\n")
	fmt.Printf("Options:\n")
	fmt.Printf("  -fq         Input fastq file (supports .gz format)\n")
	fmt.Printf("  -out        Output file path (without .gz suffix)\n")
	fmt.Printf("  -from       Source prefix to replace (default: ML15)\n")
	fmt.Printf("  -to         Target prefix to replace with (default: E251)\n")
	fmt.Printf("  -pigz       Path to pigz executable for compression\n")
	fmt.Printf("  -pigz-decompress  Use pigz for decompression (faster for gzip files)\n")
	fmt.Printf("  -version    Show version information\n\n")
	fmt.Printf("Examples:\n")
	fmt.Printf("  ml2es -fq input.fastq.gz -out output.fastq\n")
	fmt.Printf("  ml2es -fq input.fastq -out output.fastq\n")
	fmt.Printf("  ml2es -fq input.fastq -out output.fastq -from ML15 -to E251\n")
	fmt.Printf("  ml2es -fq input.fastq.gz -out output.fastq -pigz /usr/bin/pigz\n")
	fmt.Printf("  ml2es -fq input.fastq.gz -out output.fastq -from ML15 -to E251 -pigz /usr/bin/pigz\n")
	os.Exit(1)
}

// 高性能批量读取流水线模式 - 动态缓存机制 (单端reads处理)
func mainPipelineMode(fq, out, fromPrefix, toPrefix string, numWorkers int, pigzPath string) error {
	// 创建输出目录
	outDir := filepath.Dir(out)

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// 创建输入文件reader
	reader, file, err := createReader(fq)
	if err != nil {
		return err
	}
	defer file.Close()

	// 创建fastq scanner
	scanner := fastq.NewScanner(fastq.NewReader(reader))

	// 创建输出文件
	writer, outFile, err := createWriter(out)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// 创建fastq writer
	w := fastq.NewWriter(writer)

	// 创建高性能缓冲写入器，增加缓冲区大小
	bufferedW := NewBufferedFastqWriter(w, outFile, 64*1024*1024) // 64MB缓冲区
	defer bufferedW.Close()

	// 统计变量
	var totalReads int64
	var totalOutputReads int64
	var mu sync.Mutex

	// 动态缓存配置 - 针对gzip文件优化
	const workerBatchSize = 50000 // 每个worker一次处理50000条reads
	const readBatchSize = 100000  // 读取批次大小100K

	// 通道容量以“批次数”计，形成有效背压，避免内存积累
	// 固定为小批次数（numWorkers=4 时为 16 批）
	const writeCacheBatches = 16 // 4 * numWorkers
	const readQueueBatches = 16  // 4 * numWorkers

	// 固定写入批次大小 - 提升IO效率
	const writeBatchSize = 50000 // 固定50000条reads

	// 批量读取配置 - 针对gzip文件优化
	const batchQueueSize = 8 // 5-10 批，小容量以形成背压

	// 检测gzip文件并输出优化提示
	if strings.HasSuffix(fq, ".gz") {
		fmt.Printf("Gzip files detected - using optimized gzip reading strategy\n")
	}

	// 创建写入缓存池（使用有序批次，按批次数控制容量）
	writeCache := make(chan *OrderedBatch, writeCacheBatches)

	// 创建读取队列（按批次数控制容量）
	readQueue := make(chan []fastq.Sequence, readQueueBatches)

	// 批次序号计数器
	var batchSequence int64

	// 创建控制通道
	stopReading := make(chan bool)
	stopProcessing := make(chan bool)
	stopWriting := make(chan bool)

	// 批量读取配置
	batchQueue := make(chan []fastq.Sequence, batchQueueSize)

	// 启动批量读取线程
	readDone := make(chan bool)
	go func() {
		defer func() {
			readDone <- true
		}()

		batch := make([]fastq.Sequence, 0, readBatchSize)
		for scanner.Next() {
			batch = append(batch, scanner.Seq())
			if len(batch) >= readBatchSize {
				// 记录读取一个批次的时间
				start := time.Now()
				select {
				case batchQueue <- batch:
					currentReads := atomic.AddInt64(&totalReads, int64(len(batch)))
					// 每读取2M reads输出进度
					if currentReads%2000000 == 0 {
						fmt.Printf("Read %d reads\n", currentReads)
					}
					batch = make([]fastq.Sequence, 0, readBatchSize)
				case <-stopReading:
					return
				}
				fmt.Printf("[TIMING] readBatchSize=%d took=%s\n", readBatchSize, time.Since(start))
			}
		}
		// 发送最后一批
		if len(batch) > 0 {
			start := time.Now()
			select {
			case batchQueue <- batch:
				currentReads := atomic.AddInt64(&totalReads, int64(len(batch)))
				if currentReads%2000000 == 0 {
					fmt.Printf("Read %d reads (batch mode, worker batch size: %d)\n", currentReads, workerBatchSize)
				}
			case <-stopReading:
				return
			}
			fmt.Printf("[TIMING] readBatchSize=%d took=%s (last)\n", readBatchSize, time.Since(start))
		}
		close(batchQueue)
	}()

	// 启动批量处理线程 - 将reads分批发送给worker（带序号）
	processDone := make(chan bool)
	go func() {
		defer func() {
			processDone <- true
		}()

		readBatch := make([]fastq.Sequence, 0, workerBatchSize)
		for batch := range batchQueue {
			for _, seq := range batch {
				readBatch = append(readBatch, seq)
				if len(readBatch) >= workerBatchSize {
					select {
					case readQueue <- readBatch:
						readBatch = make([]fastq.Sequence, 0, workerBatchSize)
					case <-stopReading:
						return
					}
				}
			}
		}
		// 发送最后一批
		if len(readBatch) > 0 {
			select {
			case readQueue <- readBatch:
			case <-stopReading:
				return
			}
		}
		close(readQueue)
	}()

	// 创建批次序号channel，确保worker按顺序分配序号
	batchSequenceChan := make(chan int64, 100)
	go func() {
		for {
			seq := atomic.AddInt64(&batchSequence, 1)
			batchSequenceChan <- seq
		}
	}()

	// 启动worker线程池 - 批量处理（带序号以确保顺序）
	workerWg := sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer func() {
				workerWg.Done()
			}()

			for readBatch := range readQueue {
				// 获取批次序号
				batchSeq := <-batchSequenceChan

				// 批量处理reads（计时）
				start := time.Now()
				results := processReadBatch(readBatch, fromPrefix, toPrefix)
				fmt.Printf("[TIMING] worker=%d workerBatchSize=%d took=%s (seq=%d)\n", workerID, workerBatchSize, time.Since(start), batchSeq)

				// 包装为有序批次
				orderedBatch := &OrderedBatch{
					Batch:    results,
					Sequence: batchSeq,
				}

				// 发送有序批次
				select {
				case writeCache <- orderedBatch:
				case <-stopProcessing:
					return
				}
			}
		}(i)
	}

	// 启动写入协程 - 单端reads（保持顺序）
	writeDone := make(chan bool)
	go func() {
		defer func() {
			writeDone <- true
		}()

		// 有序缓冲区 - 按序号存储等待写入的批次
		orderedBuffer := make(map[int64]*OrderedBatch)
		nextSequence := int64(1)

		for orderedResult := range writeCache {
			if orderedResult == nil {
				// 结束信号，写入所有剩余批次
				for len(orderedBuffer) > 0 {
					if batch, exists := orderedBuffer[nextSequence]; exists {
						start := time.Now()
						for _, result := range batch.Batch {
							if err := bufferedW.Write(result.Seq); err != nil {
								log.Printf("Error writing: %v", err)
							}
						}
						// 更新统计
						mu.Lock()
						totalOutputReads += int64(len(batch.Batch))
						mu.Unlock()
						fmt.Printf("[TIMING] writeBatchSize=%d took=%s (seq=%d)\n", len(batch.Batch), time.Since(start), nextSequence)
						delete(orderedBuffer, nextSequence)
						nextSequence++
					} else {
						// 等待下一个批次
						break
					}
				}
				return
			}

			// 将结果放入有序缓冲区
			orderedBuffer[orderedResult.Sequence] = orderedResult

			// 尝试按顺序写入所有ready的批次
			for {
				if batch, exists := orderedBuffer[nextSequence]; exists {
					start := time.Now()
					for _, result := range batch.Batch {
						if err := bufferedW.Write(result.Seq); err != nil {
							log.Printf("Error writing: %v", err)
						}
					}
					// 更新统计
					mu.Lock()
					totalOutputReads += int64(len(batch.Batch))
					mu.Unlock()
					fmt.Printf("[TIMING] writeBatchSize=%d took=%s (seq=%d)\n", len(batch.Batch), time.Since(start), nextSequence)
					delete(orderedBuffer, nextSequence)
					nextSequence++
				} else {
					break // 等待下一批次
				}
			}
		}
	}()

	// 等待读取和处理线程完成
	<-readDone
	<-processDone

	// 关闭读取相关通道
	close(stopReading)

	// 等待所有worker完成
	workerWg.Wait()
	close(stopProcessing)

	// 发送结束信号给输出线程
	var nilBatch *OrderedBatch
	writeCache <- nilBatch

	// 等待输出线程完成
	<-writeDone
	close(stopWriting)

	// 关闭通道
	close(writeCache)

	// 强制刷新缓冲区
	fmt.Println("Flushing all buffers...")
	if err := bufferedW.Flush(); err != nil {
		fmt.Printf("Error flushing buffer: %v\n", err)
	}

	// 输出统计信息
	statsFile := out + ".stats.txt"
	statsContent := fmt.Sprintf("Total reads processed: %d\nTotal output reads: %d\n",
		totalReads, totalOutputReads)

	if err := os.WriteFile(statsFile, []byte(statsContent), 0644); err != nil {
		return fmt.Errorf("failed to write stats file: %v", err)
	}

	// 输出最终结果
	fmt.Printf("\nRead ID modification pipeline completed. Results saved to: %s\n", statsFile)
	fmt.Printf("Modified file saved to: %s\n", out)
	fmt.Printf("Total reads processed: %d, Total output reads: %d\n",
		totalReads, totalOutputReads)

	// 如果指定了pigz路径，进行压缩
	if pigzPath != "" {
		if err := compressWithPigz(out, pigzPath, numWorkers); err != nil {
			return fmt.Errorf("failed to compress output file: %v", err)
		}
	}

	return nil
}

func main() {
	// 定义命令行参数
	var fq, out, fromPrefix, toPrefix, pigzPath string
	var showVersion bool
	var usePigzDecompress bool

	flag.StringVar(&fq, "fq", "", "Input fastq file (supports .gz format)")
	flag.StringVar(&out, "out", "", "Output file path (without .gz suffix)")
	flag.StringVar(&fromPrefix, "from", DefaultFromPrefix, "Source prefix to replace")
	flag.StringVar(&toPrefix, "to", DefaultToPrefix, "Target prefix to replace with")
	flag.StringVar(&pigzPath, "pigz", "", "Path to pigz executable for compression")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.BoolVar(&usePigzDecompress, "pigz-decompress", false, "Use pigz for decompression (faster for gzip files)")

	// 解析命令行参数
	flag.Parse()

	// 显示版本信息
	if showVersion {
		fmt.Printf("ml2es v%s (Build: %s)\n", Version, BuildTime)
		os.Exit(0)
	}

	// 验证必需参数
	if fq == "" || out == "" {
		fmt.Println("Error: -fq and -out are required")
		usage()
	}

	// 固定worker数量为4
	numWorkers := 4

	// 执行高性能批量读取流水线模式
	fmt.Println("Starting read ID modification pipeline...")

	// 如果启用pigz解压缩且指定了pigz路径
	if usePigzDecompress && pigzPath != "" {
		fmt.Println("Using pigz for decompression...")

		// 解压缩gzip文件
		if strings.HasSuffix(fq, ".gz") {
			decompressedFq, err := decompressWithPigz(fq, pigzPath, numWorkers)
			if err != nil {
				log.Fatalf("Failed to decompress %s: %v", fq, err)
			}
			fq = decompressedFq
			defer os.Remove(decompressedFq) // 处理完成后删除临时文件
		}
	}

	err := mainPipelineMode(fq, out, fromPrefix, toPrefix, numWorkers, pigzPath)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

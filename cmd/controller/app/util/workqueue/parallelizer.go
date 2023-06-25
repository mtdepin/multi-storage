package workqueue

import (
	"context"
	"sync"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

type DoWorkPieceFunc func(piece int)

type options struct {
	chunkSize int
}

type Options func(*options)

// WithChunkSize allows to set chunks of work items to the workers, rather than
// processing one by one.
// It is recommended to use this option if the number of pieces significantly
// higher than the number of workers and the work done for each item is small.
func WithChunkSize(c int) func(*options) {
	return func(o *options) {
		o.chunkSize = c
	}
}

// ParallelizeUntil is a framework that allows for parallelizing N
// independent pieces of work until done or the context is canceled.
func ParallelizeUntil(ctx context.Context, workers, pieces int, doWorkPiece DoWorkPieceFunc, opts ...Options) {
	if pieces == 0 {
		return
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	chunkSize := o.chunkSize
	if chunkSize < 1 {
		chunkSize = 1
	}

	chunks := ceilDiv(pieces, chunkSize)
	toProcess := make(chan int, chunks)
	for i := 0; i < chunks; i++ {
		toProcess <- i
	}
	close(toProcess)

	var stop <-chan struct{}
	if ctx != nil {
		stop = ctx.Done()
	}
	if chunks < workers {
		workers = chunks
	}
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer utilruntime.HandleCrash()
			defer wg.Done()
			for chunk := range toProcess {
				start := chunk * chunkSize
				end := start + chunkSize
				if end > pieces {
					end = pieces
				}
				for p := start; p < end; p++ {
					select {
					case <-stop:
						return
					default:
						doWorkPiece(p)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}

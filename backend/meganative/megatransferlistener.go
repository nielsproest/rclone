package meganative

import (
	"fmt"
	"io"
	mega "rclone/megasdk"
	"sync"
)

type MyMegaTransferListener struct {
	mega.SwigDirector_MegaTransferListener
	notified bool
	err      *mega.MegaError
	transfer *mega.MegaTransfer
	m        sync.Mutex
	cv       *sync.Cond
	out      io.StringWriter
}

func (l *MyMegaTransferListener) OnTransferFinish(api mega.MegaApi, transfer mega.MegaTransfer, e mega.MegaError) {
	req := transfer.Copy()
	err := e.Copy()
	l.transfer = &req
	l.err = &err

	if err.GetErrorCode() != mega.MegaErrorAPI_OK {
		fmt.Printf("INFO: Transfer finished with error\n")
		return
	}

	l.m.Lock()
	defer l.m.Unlock()

	l.notified = true
	l.cv.Broadcast()
}

func (l *MyMegaTransferListener) OnTransferData(api mega.MegaApi, transfer mega.MegaTransfer, buffer string, size int64) bool {
	l.out.WriteString(buffer)
	return true
}

func (l *MyMegaTransferListener) GetError() *mega.MegaError {
	return l.err
}

func (l *MyMegaTransferListener) GetTransfer() *mega.MegaTransfer {
	return l.transfer
}

func (l *MyMegaTransferListener) Wait() {
	// Wait until notified becomes true
	l.m.Lock()
	defer l.m.Unlock()

	for !l.notified {
		l.cv.Wait()
	}
}

func (l *MyMegaTransferListener) Reset() {
	l.err = nil
	l.transfer = nil
	l.notified = false
}

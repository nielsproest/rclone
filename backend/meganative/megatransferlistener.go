package meganative

import (
	"fmt"
	mega "rclone/megasdk"
	"sync"
)

type BufferWriter interface {
	WriteToBuffer(data []byte) error
	EOF()
}

type MyMegaTransferListener struct {
	mega.SwigDirector_MegaTransferListener
	notified bool
	err      *mega.MegaError
	transfer *mega.MegaTransfer
	director *mega.MegaTransferListener
	m        sync.Mutex
	cv       *sync.Cond
	out      BufferWriter
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

	l.out.EOF()
}

func (l *MyMegaTransferListener) OnTransferData(api mega.MegaApi, transfer mega.MegaTransfer, buffer string, size int64) bool {
	buf := []byte(buffer)
	l.out.WriteToBuffer(buf)
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

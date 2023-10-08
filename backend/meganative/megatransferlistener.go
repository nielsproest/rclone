package meganative

import (
	"bytes"
	"fmt"
	mega "rclone/megasdk"
	"sync"
)

/*
The transfer listener for download and uploads
*/

type MyMegaTransferListener struct {
	mega.SwigDirector_MegaTransferListener
	notified bool
	err      *mega.MegaError
	transfer *mega.MegaTransfer
	director *mega.MegaTransferListener
	m        sync.Mutex
	cv       *sync.Cond
	out      *bytes.Buffer
}

func (l *MyMegaTransferListener) OnTransferFinish(api mega.MegaApi, transfer mega.MegaTransfer, e mega.MegaError) {
	req := transfer.Copy()
	err := e.Copy()
	l.transfer = &req
	l.err = &err

	if err.GetErrorCode() != mega.MegaErrorAPI_OK {
		fmt.Printf("INFO: Transfer finished with error %d - %s\n", err.GetErrorCode(), err.ToString())
	}

	{
		l.m.Lock()
		defer l.m.Unlock()

		l.notified = true
		l.cv.Broadcast()
	}
}

// Only called when "streaming"
func (l *MyMegaTransferListener) OnTransferData(api mega.MegaApi, transfer mega.MegaTransfer, buffer string) bool {
	if l.out != nil && len(buffer) > 0 {
		l.out.WriteString(buffer)
	}

	if !l.notified {
		req := transfer.Copy()
		l.transfer = &req

		tr := transfer.GetLastError()
		if tr.Swigcptr() != 0 {
			err := tr.Copy()
			l.err = &err
		}

		l.m.Lock()
		defer l.m.Unlock()

		l.notified = true
		l.cv.Broadcast()
	}

	return true
}

func (l *MyMegaTransferListener) OnTransferUpdate(api mega.MegaApi, transfer mega.MegaTransfer) {
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

package meganative

import (
	"bytes"
	"fmt"
	mega "rclone/megasdk"
	"sync"

	"github.com/rclone/rclone/fs"
)

/*
The transfer listener for download and uploads
*/

type MyMegaTransferListener struct {
	mega.SwigDirector_MegaTransferListener
	api        mega.MegaApi
	notified   bool
	err        *mega.MegaError
	transfer   *mega.MegaTransfer
	director   *mega.MegaTransferListener
	m          sync.Mutex
	cv         *sync.Cond
	buffer     *bytes.Buffer
	bufferSize int
	paused     bool
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
	if l.buffer != nil && len(buffer) > 0 {
		l.buffer.WriteString(buffer)

		if err := l.CheckOnWrite(); err != nil {
			fs.Errorf("MyMegaTransferListener", "CheckOnWrite error: %s", err.Error())
		}
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

// Set the pause state of a transfer
func (l *MyMegaTransferListener) setPause(paused bool) error {
	_transfer := l.GetTransfer()
	if _transfer == nil {
		return nil
	}
	transfer := *_transfer

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	listenerObj.Reset()
	l.api.PauseTransfer(transfer, paused, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("SetPause error: %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	return nil
}

// Check if the buffer size exceeds bufferSize
func (l *MyMegaTransferListener) CheckOnWrite() error {
	if l.paused || l.buffer.Available() < l.bufferSize {
		return nil
	}

	_transfer := l.GetTransfer()
	if _transfer == nil {
		return nil
	}
	transfer := *_transfer

	if transfer.GetState() == mega.MegaTransferSTATE_ACTIVE {
		if err := l.setPause(true); err != nil {
			fs.Debugf("MyMegaTransferListener", "Transfer resume")
			l.paused = true
		} else {
			return fmt.Errorf("transfer resume failed")
		}
	}

	return nil
}

// Check if the buffer size is less than half the buffer size
func (l *MyMegaTransferListener) CheckOnRead() error {
	if !l.paused || l.buffer.Available() > l.bufferSize/2 {
		return nil
	}

	_transfer := l.GetTransfer()
	if _transfer == nil {
		return nil
	}
	transfer := *_transfer

	if transfer.GetState() == mega.MegaTransferSTATE_ACTIVE {
		if err := l.setPause(false); err != nil {
			fs.Debugf("MyMegaTransferListener", "Transfer pause")
			l.paused = false
		} else {
			return fmt.Errorf("transfer pause failed")
		}
	}

	return nil
}

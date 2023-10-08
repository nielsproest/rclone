package meganative

import (
	"fmt"
	mega "rclone/megasdk"
	"sync"
)

type MyMegaListener struct {
	mega.SwigDirector_MegaListener
	notified bool
	err      *mega.MegaError
	request  *mega.MegaRequest
	m        sync.Mutex
	cv       *sync.Cond
	cwd      *mega.MegaNode
}

// TODO: More debug prints
func (l *MyMegaListener) OnRequestFinish(api mega.MegaApi, request mega.MegaRequest, e mega.MegaError) {
	req := request.Copy()
	err := e.Copy()
	l.request = &req
	l.err = &err

	if err.GetErrorCode() != mega.MegaErrorAPI_OK {
		fmt.Printf("INFO: Request finished with error %d - %s\n", err.GetErrorCode(), err.ToString())
	}

	l.m.Lock()
	defer l.m.Unlock()

	switch request.GetType() {
	case mega.MegaRequestTYPE_LOGIN:
		api.FetchNodes()
	case mega.MegaRequestTYPE_FETCH_NODES:
		node := api.GetRootNode()
		l.cwd = &node
		// TODO: Refetch our root also?
	}

	l.notified = true
	l.cv.Broadcast()

}

func (l *MyMegaListener) OnNodesUpdate(api mega.MegaApi, nodes mega.MegaNodeList) {
	if nodes.Swigcptr() == 0 {
		node := api.GetRootNode()
		l.cwd = &node
	}
}

func (l *MyMegaListener) OnReloadNeeded(api mega.MegaApi) {
	api.FetchNodes()
}

func (l *MyMegaListener) GetError() *mega.MegaError {
	return l.err
}

func (l *MyMegaListener) GetRequest() *mega.MegaRequest {
	return l.request
}

func (l *MyMegaListener) Wait() {
	// Wait until notified becomes true
	l.m.Lock()
	defer l.m.Unlock()

	for !l.notified {
		l.cv.Wait()
	}
}

func (l *MyMegaListener) Reset() {
	l.err = nil
	l.request = nil
	l.notified = false
}

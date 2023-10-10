package meganative

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mega "rclone/megasdk"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/readers"
)

/*
 * Stuff left to do:
 * Resolve all TODO's.
 * Test all functionality (use rclone test functions also).
 * Remove the dozen of debug calls (or make them proper debug calls with the debug fs flag),
 * Remove unnecessary pointers
 *
 * But most importantly at the end:
 * Find a way to ship this thing
 * Maybe just make a docker image
 */

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	eventWaitTime = 500 * time.Millisecond
	decayConstant = 2 // bigger for slower decay, exponential
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name: "mega_native",
		Description: `Mega Native.

The Rclone backend using the original C++ MEGA-SDK library.`,
		NewFs: NewFs,
		Options: []fs.Option{{
			Name:      "user",
			Help:      "User name.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:       "pass",
			Help:       "Password.",
			Required:   true,
			IsPassword: true,
		}, {
			Name: "cache",
			Help: `MEGA State cache folder.

Where to put MEGA state files.
These wont get deleted, so you will have to cleanup manually.`,
			Default:  "mega_cache", // TODO: Rclone already has a cache?
			Required: true,
		}, {
			Name: "loglevel",
			Help: `Set the active log level

These are the valid values for this parameter:
 - MegaApi::LOG_LEVEL_FATAL = 0
 - MegaApi::LOG_LEVEL_ERROR = 1
 - MegaApi::LOG_LEVEL_WARNING = 2
 - MegaApi::LOG_LEVEL_INFO = 3
 - MegaApi::LOG_LEVEL_DEBUG = 4
 - MegaApi::LOG_LEVEL_MAX = 5
This function sets the log level of the logging system. Any log listener registered by
MegaApi::addLoggerObject will receive logs with the same or a lower level than
the one passed to this function.`,
			Default:  2,
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: `Delete files permanently rather than putting them into the trash.

Normally the mega backend will put all deletions into the trash rather
than permanently deleting them. If you specify this then rclone will
permanently delete objects instead.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "use_https",
			Help: `Use HTTPS communications only

The default behavior is to use HTTP for transfers and the persistent connection
to wait for external events. Those communications don't require HTTPS because
all transfer data is already end-to-end encrypted and no data is transmitted
over the connection to wait for events (it's just closed when there are new events).

This feature should only be enabled if there are problems to contact MEGA servers
through HTTP because otherwise it doesn't have any benefit and will cause a
higher CPU usage.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "worker_threads",
			Help: `The number of worker threads for encryption or other operations.

Using worker threads means that synchronous function calls on MegaApi will be blocked less,
and uploads and downloads can proceed more quickly on very fast connections.`,
			Default:  2,
			Advanced: true,
		}, {
			Name: "download_cache",
			Help: `Sets how big the download cache should be in MB

This is a loose limit, as it only check if we go beyond said value.
The minimum is 1, but this will make your transfers really slow.
It is recommended to keep it above 32`,
			Default:  64,
			Advanced: true,
		}, {
			Name:     "download_concurrency",
			Help:     `Set the maximum number of connections per transfer for downloads`,
			Default:  1,
			Advanced: true,
		}, {
			Name:     "upload_concurrency",
			Help:     `Set the maximum number of connections per transfer for uploads`,
			Default:  1,
			Advanced: true,
		}, {
			Name: "download_min_rate",
			Help: `Set the miniumum acceptable streaming speed for streaming transfers

When streaming a file with startStreaming(), the SDK monitors the transfer rate.
After a few seconds grace period, the monitoring starts. If the average rate is below
the minimum rate specified (determined by this function, or by default a reasonable rate
for audio/video, then the streaming operation will fail with MegaError::API_EAGAIN.

Use -1 to use the default built into the library.
Use 0 to prevent the check.`,
			Default:  -1,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// Encode invalid UTF-8 bytes as json doesn't handle them properly.
			Default: (encoder.Base |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

/*
 * MEGA-SDK Path info:
 * The path separator character is '/'
 * The Root node is /
 * The Vault root node is //in/
 * The Rubbish root node is //bin/
 *
 * Paths with names containing '/', '\' or ':' aren't compatible
 * with this function.
 */

// Options defines the configuration for this backend
type Options struct {
	User            string               `config:"user"`
	Pass            string               `config:"pass"`
	LogLevel        int                  `config:"loglevel"`
	Cache           string               `config:"cache"`
	HardDelete      bool                 `config:"hard_delete"`
	UseHTTPS        bool                 `config:"use_https"`
	WorkerThreads   int                  `config:"worker_threads"`
	DownloadCache   int                  `config:"download_cache"`
	DownloadThreads int                  `config:"download_concurrency"`
	UploadThreads   int                  `config:"upload_concurrency"`
	DownloadMinRate int                  `config:"download_min_rate"`
	Enc             encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote mega
type Fs struct {
	name      string // name of this remote
	root      string // the path we are working on
	_rootNode *mega.MegaNode
	opt       Options         // parsed config options
	pacer     *fs.Pacer       // pacer for API calls
	features  *fs.Features    // optional features
	srv       *mega.MegaApi   // the connection to the server
	srvListen *MyMegaListener // the request listener
}

// Object describes a mega object
//
// Will definitely have info but maybe not meta.
//
// Normally rclone would just store an ID here but go-mega and mega.nz
// expect you to build an entire tree of all the objects in memory.
// In this case we just store a pointer to the object.
type Object struct {
	fs     *Fs    // what this object is part of
	remote string // The remote path
	handle int64  // The node identifier
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("mega root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// parsePath parses a mega 'url'
func (f *Fs) parsePath(path string) (string, *mega.MegaNode) {
	return strings.Trim(path, "/"), f._rootNode
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.Pass != "" {
		var err error
		opt.Pass, err = obscure.Reveal(opt.Pass)
		if err != nil {
			return nil, fmt.Errorf("couldn't decrypt password: %w", err)
		}
	}

	// Loglevel
	mega.MegaApiSetLogLevel(opt.LogLevel)
	mega.MegaApiSetLogToConsole(true)
	// TODO: Add dedicated logger instead
	/*loggerObj := MyMegaLogListener{}
	logger := mega.NewDirectorMegaLogger(&loggerObj)
	mega.MegaApiAddLoggerObject(logger)*/

	// Used for api calls, not transfers
	listenerObj, listener := getRequestListener()

	// Check if the directory exists
	_, err = os.Stat(opt.Cache)
	// If the directory doesn't exist, create it
	if os.IsNotExist(err) {
		err := os.MkdirAll(opt.Cache, 0755) // 0755 sets the directory permissions
		if err != nil {
			return nil, fmt.Errorf("error creating directory: %w", err)
		}
		fs.Debugf("mega-native", "Directory created: %s", opt.Cache)
	} else if err != nil {
		return nil, fmt.Errorf("error checking directory: %w", err)
	} else {
		fs.Debugf("mega-native", "Directory already exists: %s", opt.Cache)
	}

	// TODO: Generate code at: https://mega.co.nz/#sdk
	srv := mega.NewMegaApi(
		"ht1gUZLZ", // appKey
		opt.Cache,  // basePath
		fmt.Sprintf("MEGA/SDK Rclone FS %s", fs.Version), // userAgent
		uint(opt.WorkerThreads))                          // workerThreadCount
	srv.AddRequestListener(listener)
	srv.SetStreamingMinimumRate(opt.DownloadMinRate)

	// Use HTTPS
	listenerObj.Reset()
	srv.UseHttpsOnly(opt.UseHTTPS, listener)
	listenerObj.Wait()

	if _, err := GetMegaError(listenerObj, "UseHttpsOnly"); err != nil {
		fs.Errorf("mega-native", "%w", err)
	}

	// DL Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_DOWNLOAD, opt.DownloadThreads, listener)
	listenerObj.Wait()

	if _, err := GetMegaError(listenerObj, "SetMaxConnections Downloads"); err != nil {
		fs.Errorf("mega-native", "%w", err)
	}

	// UP Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_UPLOAD, opt.UploadThreads, listener)
	listenerObj.Wait()

	if _, err := GetMegaError(listenerObj, "SetMaxConnections Uploads"); err != nil {
		fs.Errorf("mega-native", "%w", err)
	}

	// TODO: Use debug for something
	fs.Logf("mega-native", "Trying log in...")

	listenerObj.Reset()
	// TODO: multiFactorAuthCheck
	srv.Login(opt.User, opt.Pass)
	listenerObj.Wait()

	if _, err := GetMegaError(listenerObj, "Login"); err != nil {
		return nil, err
	}

	fs.Logf("mega-native", "Waiting for nodes...")
	for listenerObj.cwd == nil {
		time.Sleep(100 * time.Millisecond)
	}

	root = filepath.Join("/", strings.TrimSuffix(root, "/"))
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		srv:       &srv,
		srvListen: listenerObj,
		pacer:     fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true,
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)

	// Find the root node and check if it is a file or not
	fs.Logf("mega-native", "Retrieving root node...")
	rootNode := f.API().GetNodeByPath(f.root)

	// This occurs in "move" operations, where the destionation filesystem shouldn't have a "root"
	if rootNode.Swigcptr() == 0 {
		fs.Debugf("mega-native", "Couldn't find root node!")
		return f, nil
	}

	/* TODO: Fix cache bug:
	[14:37:49][err] Unable to FileAccess::fopen('./mega_cache/jid'): sysstat() failed: error code: 2: No such file or directory
	2023/10/06 16:37:49 NOTICE: mega-native: Trying log in...
	[14:37:50][err] Failed to open('./mega_cache/jid'): error 2: No such file or directory
	[14:37:50][err] [MegaClient::JourneyID::resetCacheAndValues] Unable to remove local cache file
	[14:37:50][err] [MegaClient::JourneyID::loadValuesFromCache] Unable to load values from the local cache
	*/

	switch rootNode.GetType() {
	case mega.MegaNodeTYPE_FOLDER:
		// root node found and is a directory
	case mega.MegaNodeTYPE_FILE:
		// root node is a file so point to parent directory
		root = path.Dir(root)
		if root == "." {
			root = ""
		}
		f.root = root
		return f, fs.ErrorIsFile
	default:
		// unknown
	}
	return f, nil
}

// ------------------------------------------------------------
// Support functions

// Just a macro
func (f *Fs) API() mega.MegaApi {
	return *f.srv
}

type HasMegaError interface {
	GetError() *mega.MegaError
}

func GetMegaError(listener HasMegaError, operation string) (mega.MegaError, error) {
	_merr := listener.GetError()
	if _merr == nil {
		return nil, nil
	}
	merr := *_merr

	if merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return merr, fmt.Errorf("MEGA %s Error: %d - %s", operation, merr.GetErrorCode(), merr.ToString())
	}

	return merr, nil
}

// shouldRetry returns a boolean as to whether this err deserves to be
// retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	// Let the mega library handle the low level retries
	return false, err
}

// Converts any mega unix time to time.Time
func intToTime(num int64) time.Time {
	return time.Unix(num, 0)
}

// getObject looks up the node for the path of the name given from the root given
//
// It returns mega.ENOENT if it wasn't found
func (f *Fs) getObject(dir string) (mega.MegaNode, error) {
	fs.Debugf(f, "getObject %s", dir)

	trimmedDir, _rootNode := f.parsePath(dir)
	if _rootNode == nil || (*_rootNode).Swigcptr() == 0 {
		return nil, fmt.Errorf("root not found")
	}

	node := f.API().GetNodeByPath(trimmedDir, *_rootNode)
	if node.Swigcptr() == 0 {
		return nil, fs.ErrorObjectNotFound
	}

	return node, nil
}

// findDir finds the directory rooted from the node passed in
func (f *Fs) findDir(dir string) (*mega.MegaNode, error) {
	fs.Debugf(f, "findDir %s", dir)

	n, err := f.getObject(dir)
	if err != nil {
		if err == fs.ErrorObjectNotFound {
			err = fs.ErrorDirNotFound
		}
		return nil, err
	}

	if n.Swigcptr() != 0 && n.GetType() == mega.MegaNodeTYPE_FILE {
		return nil, fs.ErrorIsFile
	}

	return &n, nil
}

// findObject looks up the node for the object of the name given
func (f *Fs) findObject(file string) (*mega.MegaNode, error) {
	fs.Debugf(f, "findObject %s", file)

	n, err := f.getObject(file)
	if err != nil {
		return nil, err
	}

	if n.Swigcptr() != 0 && n.GetType() != mega.MegaNodeTYPE_FILE {
		return nil, fs.ErrorIsDir
	}
	return &n, nil
}

// Create request listener (remember to destroy it after)
func getRequestListener() (*MyMegaListener, mega.MegaRequestListener) {
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)
	return &listenerObj, listener
}

// Create transfer listener (remember to destroy it after)
func (f *Fs) getTransferListener() (*MyMegaTransferListener, mega.MegaTransferListener) {
	buf := new(bytes.Buffer)
	listenerObj := MyMegaTransferListener{
		buffer:     buf,
		bufferSize: 1024 * 1024 * f.opt.DownloadCache,
		api:        *f.srv,
	}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)
	return &listenerObj, listener
}

// Permanently deletes a node
func (f *Fs) hardDelete(ctx context.Context, node mega.MegaNode) error {
	fs.Debugf(f, "hardDelete %s", node.GetName())

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Hard deletes a node
	err := f.pacer.Call(func() (bool, error) {
		listenerObj.Reset()
		f.API().Remove(node, listener)
		listenerObj.Wait()

		_, err := GetMegaError(listenerObj, "NodeRemove")
		return shouldRetry(ctx, err)
	})

	if err != nil {
		return fmt.Errorf("remove node failed: %w", err)
	}

	return nil
}

// Deletes a node
func (f *Fs) delete(ctx context.Context, node mega.MegaNode) error {
	fs.Debugf(f, "delete %s", node.GetName())

	if f.opt.HardDelete {
		if err := f.hardDelete(ctx, node); err != nil {
			return err
		}
	} else {
		trashNode := f.API().GetRubbishNode()
		if trashNode.Swigcptr() == 0 {
			return fmt.Errorf("trash node was null")
		}

		if err := f.moveNode(ctx, node, trashNode); err != nil {
			return err
		}
	}

	return nil
}

// Moves a node into a directory node
func (f *Fs) moveNode(ctx context.Context, node mega.MegaNode, dir mega.MegaNode) error {
	fs.Debugf(f, "moveNode %s -> %s/", node.GetName(), dir.GetName())

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Moves a node into another node
	err := f.pacer.Call(func() (bool, error) {
		listenerObj.Reset()
		f.API().MoveNode(node, dir, listener)
		listenerObj.Wait()

		_, err := GetMegaError(listenerObj, "MoveNode")
		return shouldRetry(ctx, err)
	})

	if err != nil {
		return fmt.Errorf("move node failed: %w", err)
	}

	return nil
}

// Rename a node
func (f *Fs) renameNode(ctx context.Context, node mega.MegaNode, name string) error {
	fs.Debugf(f, "rename %s -> %s", node.GetName(), name)

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Rename node
	err := f.pacer.Call(func() (bool, error) {
		listenerObj.Reset()
		f.API().RenameNode(node, name, listener)
		listenerObj.Wait()

		_, err := GetMegaError(listenerObj, "RenameNode")
		return shouldRetry(ctx, err)
	})

	if err != nil {
		return fmt.Errorf("rename node failed: %w", err)
	}

	return nil
}

// Creates a root if missing
func (f *Fs) rootFix(ctx context.Context) error {
	if f._rootNode == nil {
		root, rootName := filepath.Split(f.root)
		rootNode := f.API().GetNodeByPath(root)
		if rootNode.Swigcptr() == 0 {
			return fs.ErrorDirNotFound
		}

		listenerObj, listener := getRequestListener()
		defer mega.DeleteDirectorMegaRequestListener(listener)

		// Create folder
		// TODO: Pacer
		f.API().CreateFolder(rootName, rootNode, listener)
		listenerObj.Wait()

		if _, err := GetMegaError(listenerObj, "CreateFolder"); err != nil {
			return err
		}

		rootNode = f.API().GetNodeByPath(f.root)
		if rootNode.Swigcptr() == 0 {
			return fmt.Errorf("root dir created but non-existant")
		}

		f._rootNode = &rootNode
	}
	return nil
}

// Make the parent directory of a path and return it
func (f *Fs) mkdirParent(ctx context.Context, path string) (*mega.MegaNode, error) {
	fs.Debugf(f, "mkdirParent %s", path)

	err := f.rootFix(ctx)
	if err != nil {
		return nil, err
	}

	path, _rNode := f.parsePath(path)
	if _rNode == nil {
		return nil, fmt.Errorf("root not found")
	}
	rootNode := *_rNode

	// If parent dir exists
	_root, _ := filepath.Split(path)
	_rootNode := f.API().GetNodeByPath(_root, rootNode)

	if _rootNode.Swigcptr() != 0 {
		return &_rootNode, fs.ErrorDirExists
	}

	// If parent dir doesnt exist
	_rootParent, _rootName := filepath.Split(_root)
	_rootParentNode := f.API().GetNodeByPath(_rootParent, rootNode)
	if _rootParentNode.Swigcptr() != 0 {
		return nil, fs.ErrorDirNotFound
	}

	return f.mkdir(ctx, _rootName, &_rootParentNode)
}

// Iterate over the children of a node
func (f *Fs) iterChildren(node mega.MegaNode) (<-chan mega.MegaNode, error) {
	dataCh := make(chan mega.MegaNode)

	children := f.API().GetChildren(node)
	if children.Swigcptr() == 0 {
		return dataCh, fs.ErrorListAborted
	}

	go func() {
		defer close(dataCh)
		for i := 0; i < children.Size(); i++ {
			// Children are destroyed if the parent is free'd
			// Ensure this doesnt happen by making a copy
			dataCh <- children.Get(i).Copy()
		}
	}()

	return dataCh, nil
}

// Make directory and return it
func (f *Fs) mkdir(ctx context.Context, name string, parent *mega.MegaNode) (*mega.MegaNode, error) {
	fs.Debugf(f, "mkdir %s/%s", (*parent).GetName(), name)

	err := f.rootFix(ctx)
	if err != nil {
		return nil, err
	}

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Create folder
	err = f.pacer.Call(func() (bool, error) {
		listenerObj.Reset()
		f.API().CreateFolder(name, *parent, listener)
		listenerObj.Wait()

		_, err := GetMegaError(listenerObj, "CreateFolder")
		return shouldRetry(ctx, err)
	})

	if err != nil {
		return nil, fmt.Errorf("create folder node failed: %w", err)
	}

	// Find resulting folder
	children, err := f.iterChildren(*parent)
	if err != nil {
		return nil, err
	}

	// TODO: Can directories and files have the same name?
	for node := range children {
		if node.GetName() == name && node.GetType() == mega.MegaNodeTYPE_FOLDER {
			return &node, nil
		}
	}

	return nil, fmt.Errorf("created folder not found")
}

// ------------------------------------------------------------

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "List %s", dir)
	f.rootFix(ctx)

	dirNode, err := f.findDir(dir)
	if err != nil {
		return nil, err
	}

	children, err := f.iterChildren(*dirNode)
	if err != nil {
		return nil, err
	}

	for node := range children {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(node.GetName()))
		switch node.GetType() {
		case mega.MegaNodeTYPE_FOLDER:
			modTime := intToTime(node.GetCreationTime())
			d := fs.NewDir(remote, modTime).SetID(node.GetBase64Handle()).SetParentID((*dirNode).GetBase64Handle())
			entries = append(entries, d)
		case mega.MegaNodeTYPE_FILE:
			o := &Object{
				fs:     f,
				remote: remote,
				handle: node.GetHandle(),
			}
			entries = append(entries, o)
		}
	}

	return entries, nil
}

// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fs.Debugf(f, "Mkdir %s", dir)

	_, err := f.findDir(dir)
	if err == nil {
		return fs.ErrorDirExists
	}

	srcPath, srcName := filepath.Split(dir)
	n, err := f.findDir(srcPath)
	if err != nil {
		return err
	}
	if (*n).IsFile() {
		return fs.ErrorIsFile
	}

	_, err = f.mkdir(ctx, srcName, n)
	return err
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "NewObject %s", remote)
	f.rootFix(ctx)

	o := &Object{
		fs:     f,
		remote: remote,
	}

	_, err := f.mkdirParent(ctx, remote)
	if err != nil && err != fs.ErrorDirExists {
		return o, err
	}

	n, err := f.getObject(remote)
	if err != nil {
		return o, err
	}

	o.handle = n.GetHandle()

	return o, err
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Put the object
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Debugf(f, "Put %s", src.Remote())

	existingObj, err := f.findObject(src.Remote())
	if err != nil {
		return f.PutUnchecked(ctx, in, src, options...)
	}

	o := &Object{
		fs:     f,
		remote: src.Remote(),
		handle: (*existingObj).GetHandle(),
	}

	return o, o.Update(ctx, in, src, options...)
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	fs.Debugf(f, "Rmdir %s", dir)

	n, err := f.findDir(dir)
	if err != nil {
		return err
	}

	if !(*n).IsFolder() {
		return fmt.Errorf("the path isn't a folder")
	}

	// TODO: Should i do this?
	/*if c := (*n).GetChildren(); c.Swigcptr() != 0 || c.Size() > 0 {
		return fmt.Errorf("folder not empty")
	}*/

	if err := f.delete(ctx, *n); err != nil {
		return err
	}

	fs.Debugf(f, "Folder deleted OK")
	return nil
}

// ------------------------------------------------------------

// CustomReadCloser is a custom implementation of io.ReadCloser
type CustomReadCloser struct {
	io.Reader
	closed   bool
	closeMu  sync.Mutex
	listener *MyMegaTransferListener
	fs       *Fs
}

// NewCustomReadCloser creates a new CustomReadCloser
func NewCustomReadCloser(r io.Reader) *CustomReadCloser {
	return &CustomReadCloser{
		Reader: io.NopCloser(r),
	}
}

// Read overrides the Read method to add additional checks
func (crc *CustomReadCloser) Read(p []byte) (n int, err error) {
	// If the reader ended with an error
	/*if crc.listener.done && crc.listener.err != nil {
		if merr, err := GetMegaError(crc.listener, "Download"); err != nil {
			if merr.GetErrorCode() ==  && !crc.closed {
				crc.Close()
			}
			// return 0, err
		}
	}*/

	// Read from NopCloser
	n, err = crc.Reader.Read(p)

	// Un-throttle if appropriate
	if err := crc.listener.CheckOnRead(); err != nil {
		fs.Errorf("MyMegaTransferListener", "CheckOnRead error: %w", err)
	}

	return n, err
}

// Close overrides the Close method to track when it's called
func (crc *CustomReadCloser) Close() error {
	crc.closeMu.Lock()
	defer crc.closeMu.Unlock()

	if crc.closed {
		return fmt.Errorf("already closed") // Avoid double closing
	}

	// Check for any errors
	if _, err := GetMegaError(crc.listener, "Download"); err != nil {
		fs.Errorf(crc.fs, "%w", err)
	}

	// Ask Mega kindly to stop
	transfer := crc.listener.GetTransfer()
	if transfer != nil {
		_transfer := *transfer
		if _transfer.Swigcptr() != 0 {
			crc.fs.API().CancelTransfer(_transfer)
		}
	}

	// Close
	crc.closed = true
	fmt.Println("Close method called on CustomReadCloser")
	return nil
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	f := o.fs
	fs.Debugf(o, "File Open %s", o.Remote())

	var offset, limit int64 = 0, -1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	// Fixes
	if o.Size() <= offset {
		return nil, io.EOF
	}
	if 0 > offset {
		offset = 0
	}
	if 0 >= limit || limit+offset > o.Size() {
		limit = o.Size() - offset
	}

	// Create listener
	listenerObj, listener := f.getTransferListener()

	// TODO: vfs writes is still a little buggy, aint my fault windows opens it 300 god damn times
	reader := NewCustomReadCloser(listenerObj.buffer)
	reader.listener = listenerObj
	reader.fs = f

	// Start streamer and wait for first response
	err := f.pacer.Call(func() (bool, error) {
		// Get node referencce
		_node, err := o.getRef()
		if err != nil {
			return shouldRetry(ctx, err)
		}

		listenerObj.Reset()
		f.API().StartStreaming(*_node, offset, limit, listener)
		listenerObj.WaitStream()

		_, err = GetMegaError(listenerObj, "StartStreaming")
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, fmt.Errorf("open download file failed: %w", err)
	}

	return readers.NewLimitedReadCloser(reader, limit), nil
}

// ------------------------------------------------------------

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	fs.Debugf(o.fs, "Remove %s", o.Remote())

	n, err := o.getRef()
	if err != nil {
		return err
	}

	if !(*n).IsFile() {
		return fs.ErrorIsDir
	}

	if err := o.fs.delete(ctx, *n); err != nil {
		return err
	}

	fs.Debugf(o.fs, "File deleted OK")
	return nil
}

func (o *Object) getRef() (*mega.MegaNode, error) {
	node := o.fs.API().GetNodeByHandle(o.handle)
	if node.Swigcptr() == 0 {
		return &node, fs.ErrorObjectNotFound
	}
	return &node, nil
}

// ------------------------------------------------------------

// Purge deletes all the files in the directory
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	fs.Debugf(f, "Purge %s", dir)

	n, err := f.findDir(dir)
	if err != nil {
		return err
	}

	if !(*n).IsFolder() {
		return fs.ErrorIsFile
	}

	// TODO: Should i do this?
	/*if c := (*n).GetChildren(); c.Swigcptr() != 0 || c.Size() > 0 {
		return fmt.Errorf("folder not empty")
	}*/

	if err := f.delete(ctx, *n); err != nil {
		return err
	}

	fs.Debugf(f, "Folder purged OK")
	return nil
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (dstFs *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	fs.Debugf(dstFs, "Move")

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(dstFs, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	srcFs := srcObj.fs

	srcNode, err := srcObj.getRef()
	if err != nil {
		return nil, err
	}

	dest, err := dstFs.findObject(remote)
	if err == nil && (*dest).IsFile() {
		fs.Debugf(dstFs, "The destination is an existing file")
		return nil, fs.ErrorIsFile
	}

	destNode, err := dstFs.mkdirParent(ctx, remote)
	if err != nil && err != fs.ErrorDirExists {
		fs.Debugf(dstFs, "Destination folder not found")
		return nil, err
	}

	absSrc, _ := srcFs.parsePath(src.Remote())
	absDst, _ := dstFs.parsePath(remote)
	absSrc = filepath.Join(srcFs.root, absSrc)
	absDst = filepath.Join(dstFs.root, absDst)
	fs.Debugf(dstFs, "Move %q -> %q", absSrc, absDst)
	srcPath, srcName := filepath.Split(absSrc)
	dstPath, dstName := filepath.Split(absDst)

	if srcPath != dstPath {
		if err := dstFs.moveNode(ctx, *srcNode, *destNode); err != nil {
			return nil, err
		}
	}
	if srcName != dstName {
		if err := dstFs.renameNode(ctx, *srcNode, dstName); err != nil {
			return nil, err
		}
	}

	srcObj.remote = remote
	return srcObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (dstFs *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	fs.Debugf(dstFs, "DirMove")
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcNode, err := srcFs.findDir(srcRemote)
	if err != nil {
		return err
	}

	dstNode, err := dstFs.mkdirParent(ctx, dstRemote)
	if err != nil && err != fs.ErrorDirExists {
		return err
	}

	absSrc, _ := srcFs.parsePath(srcRemote)
	absDst, _ := dstFs.parsePath(dstRemote)
	absSrc = filepath.Join(srcFs.root, absSrc)
	absDst = filepath.Join(dstFs.root, absDst)
	fs.Debugf(dstFs, "DirMove: %s -> %s", absSrc, absDst)
	srcPath, srcName := filepath.Split(absSrc)
	destPath, destName := filepath.Split(absDst)

	if srcPath != destPath {
		if err := dstFs.moveNode(ctx, *srcNode, *dstNode); err != nil {
			return err
		}
	}
	if srcName != destName {
		if err := dstFs.renameNode(ctx, *srcNode, destName); err != nil {
			return err
		}
	}

	return nil
}

// MergeDirs merges the contents of all the directories passed
// in into the first one and rmdirs the other directories.
func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) error {
	fs.Debugf(f, "MergeDirs")

	// TODO: Test this

	if len(dirs) < 2 {
		return nil
	}

	// find dst directory
	dstDir := dirs[0]
	dstNode := f.API().GetNodeByHandle(mega.MegaApiBase64ToHandle(dstDir.ID()))
	if dstNode.Swigcptr() == 0 {
		return fs.ErrorDirNotFound
	}

	for _, srcDir := range dirs[1:] {
		srcNode := f.API().GetNodeByHandle(mega.MegaApiBase64ToHandle(srcDir.ID()))
		if srcNode.Swigcptr() == 0 {
			return fs.ErrorObjectNotFound
		}

		children, err := f.iterChildren(srcNode)
		if err != nil {
			return err
		}

		for node := range children {
			if err := f.moveNode(ctx, node, dstNode); err != nil {
				return err
			}
		}

		if err := f.delete(ctx, srcNode); err != nil {
			return err
		}
	}

	return nil
}

// ------------------------------------------------------------

// About gets quota information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	fs.Debugf(f, "About")

	var data mega.MegaRequest
	{
		listenerObj, listener := getRequestListener()
		defer mega.DeleteDirectorMegaRequestListener(listener)

		// Get account details
		err := f.pacer.Call(func() (bool, error) {
			listenerObj.Reset()
			f.API().GetAccountDetails(listener)
			listenerObj.Wait()

			_, err := GetMegaError(listenerObj, "GetAccountDetails")
			return shouldRetry(ctx, err)
		})

		if err != nil {
			return nil, fmt.Errorf("GetAccountDetails failed: %w", err)
		}

		_data := listenerObj.GetRequest()
		if _data == nil {
			return nil, fmt.Errorf("GetAccountDetails request is null")
		}
		data = *_data
	}

	if data.GetType() != mega.MegaRequestTYPE_ACCOUNT_DETAILS {
		return nil, fmt.Errorf("request is wrong type %d != %d", mega.MegaRequestTYPE_ACCOUNT_DETAILS, data.GetType())
	}

	account_details := data.GetMegaAccountDetails()
	if account_details.Swigcptr() == 0 {
		return nil, fmt.Errorf("GetMegaAccountDetails returned null")
	}

	usage := &fs.Usage{
		Total: fs.NewUsageValue(account_details.GetStorageMax()),                                    // quota of bytes that can be used
		Used:  fs.NewUsageValue(account_details.GetStorageUsed()),                                   // bytes in use
		Free:  fs.NewUsageValue(account_details.GetStorageMax() - account_details.GetStorageUsed()), // bytes which can be uploaded before reaching the quota
	}

	rootNode := f.API().GetRootNode()
	if rootNode.Swigcptr() != 0 {
		// TODO: Could also be: f.API().GetNumNodes() ?
		// TODO: Is this on our root or globally (currently its globally)
		usage.Objects = fs.NewUsageValue(account_details.GetNumFiles(rootNode.GetHandle())) // objects in the storage system
	}

	trashNode := f.API().GetRubbishNode()
	if trashNode.Swigcptr() != 0 {
		usage.Trashed = fs.NewUsageValue(account_details.GetStorageUsed(trashNode.GetHandle())) // bytes in trash
	}

	vaultNode := f.API().GetVaultNode()
	if vaultNode.Swigcptr() != 0 {
		usage.Other = fs.NewUsageValue(account_details.GetStorageUsed(vaultNode.GetHandle())) // other usage e.g. gmail in drive
	}

	return usage, nil
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	fs.Debugf(o.fs, "String %s", o.Remote())
	path, _ := o.fs.parsePath(o.remote)
	return path
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash implements fs.Object.
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	fs.Debugf(o.fs, "Hash %s", o.Remote())
	// TODO: Is reportedly a "Base64-encoded CRC of the file. The CRC of a file is a hash of its contents"
	// But i cant figure it out

	node, err := o.getRef()
	if err != nil {
		return "", err
	}

	return o.fs.API().GetCRC(*node), nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	fs.Debugf(o.fs, "Size %s", o.remote)

	node, err := o.getRef()
	if err != nil {
		return -1
	}

	return (*node).GetSize()
}

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	fs.Debugf(f, "PublicLink")

	node, err := f.getObject(remote)
	if err != nil {
		return "", err
	}

	link := node.GetPublicLink(true)
	if link == "" {
		err = fmt.Errorf("non-exported file")
	}

	return link, err
}

// DirCacheFlush resets the directory cache - used in testing
// as an optional interface
func (f *Fs) DirCacheFlush() {
	// TODO: f.API().Catchup(listener) ?
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// PutUnchecked the object
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Debugf(f, "PutUnchecked %s", src.Remote())

	dstObj := &Object{
		fs:     f,
		remote: src.Remote(),
	}

	return dstObj, dstObj.Update(ctx, in, src, options...)
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one.
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	fs.Debugf(f, "Update %s", o.Remote())

	_parentNode, err := f.mkdirParent(ctx, o.remote)
	if err != nil && err != fs.ErrorDirExists {
		fs.Debugf(f, "Parent folder creation failed")
		return err
	}
	parentNode := (*_parentNode).Copy()

	// Create a temporary file
	// Somebody contact MEGA, ask them for a upload chunks interface :P
	tempFile, err := os.CreateTemp(f.opt.Cache, "mega*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file")
	}
	defer os.Remove(tempFile.Name())

	// Unlike downloads, there is no upload bytes interface
	// It must be a file, so we create a temporary one
	_, err = io.Copy(tempFile, in)
	if err != nil {
		return fmt.Errorf("failed to write temporary file")
	}

	// Create listener
	listenerObj, listener := f.getTransferListener()
	defer mega.DeleteDirectorMegaTransferListener(listener)

	// Create cancel token
	token := mega.MegaCancelTokenCreateInstance()
	defer mega.DeleteMegaCancelToken(token)

	// Upload
	_, fileName := filepath.Split(o.remote)
	err = f.pacer.Call(func() (bool, error) {
		listenerObj.Reset()
		f.API().StartUpload(
			tempFile.Name(),         //Localpath
			parentNode,              //Directory
			fileName,                //Filename
			src.ModTime(ctx).Unix(), //Modification time
			f.opt.Cache,             // Temporary directory
			false,                   // Temporary source
			false,                   // Priority
			token,                   // Cancel token
			listener,                //Listener
		)
		listenerObj.Wait()

		_, err := GetMegaError(listenerObj, "StartUpload")
		return shouldRetry(ctx, err)
	})

	if err != nil {
		return fmt.Errorf("StartUpload failed: %w", err)
	}

	// Set metadata
	// TODO: Do i have to do this?
	mobj := listenerObj.GetTransfer()
	if mobj != nil {
		node := f.API().GetNodeByHandle((*mobj).GetNodeHandle())
		if node.Swigcptr() != 0 {
			o.handle = node.GetHandle()
		}
	}

	return err
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(context.Context) time.Time {
	fs.Debugf(o.fs, "ModTime %s", o.Remote())

	node, err := o.getRef()
	if err != nil {
		return time.Unix(0, 0)
	}

	return intToTime((*node).GetModificationTime())
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	fs.Debugf(o.fs, "ID %s", o.Remote())

	node, err := o.getRef()
	if err != nil {
		return ""
	}

	return (*node).GetBase64Handle()
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.PublicLinker    = (*Fs)(nil)
	_ fs.MergeDirser     = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)

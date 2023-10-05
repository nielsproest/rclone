package meganative

import (
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
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/readers"
)

/*
 * Stuff left to do:
 * Resolve all TODO's.
 * Test all functionality (use rclone test functions also).
 * Remove the dozen of debug calls (or make them proper debug calls with the debug fs flag),
 * actually i should find if MEGA has optional debug flags i can set.
 * Potentially find a better upload method.
 *
 * But most importantly at the end:
 * Find a way to ship this thing
 */

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name: "mega_native",
		Description: `Mega Native.

The MEGA-SDK using the original C++ SDK's library.`,
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

TODO: Rclone already has a cache?
Where to put MEGA state files.
These wont get deleted, so you will have to cleanup manually.`,
			Default:  "mega_cache",
			Required: true,
		}, {
			Name: "debug",
			Help: `Output more debug from Mega.

If this flag is set (along with -vv) it will print further debugging
information from the mega backend.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: `Delete files permanently rather than putting them into the trash.

Normally the mega backend will put all deletions into the trash rather
than permanently deleting them.  If you specify this then rclone will
permanently delete objects instead.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "use_https",
			Help: `Use HTTPS for transfers.

MEGA uses plain text HTTP connections by default.
Some ISPs throttle HTTP connections, this causes transfers to become very slow.
Enabling this will force MEGA to use HTTPS for all transfers.
HTTPS is normally not necessary since all data is already encrypted anyway.
Enabling it will increase CPU usage and add network overhead.`,
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
			Name:     "download_threads",
			Help:     `Set the maximum number of connections per transfer for downloads`,
			Default:  3,
			Advanced: true,
		}, {
			Name:     "upload_threads",
			Help:     `Set the maximum number of connections per transfer for uploads`,
			Default:  1,
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
	Debug           bool                 `config:"debug"`
	Cache           string               `config:"cache"`
	HardDelete      bool                 `config:"hard_delete"`
	UseHTTPS        bool                 `config:"use_https"`
	WorkerThreads   int                  `config:"worker_threads"`
	DownloadThreads int                  `config:"download_threads"`
	UploadThreads   int                  `config:"upload_threads"`
	Enc             encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote mega
type Fs struct {
	name      string // name of this remote
	root      string // the path we are working on
	_rootNode *mega.MegaNode
	opt       Options         // parsed config options
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
	handle int64
	// info   *mega.MegaNode // pointer to the mega node
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

	// Used for api calls, not transfers
	listenerObj, listener := getRequestListener()

	// TODO: Generate code at: https://mega.co.nz/#sdk
	srv := mega.NewMegaApi(
		"ht1gUZLZ",                   // appKey
		opt.Cache,                    // basePath
		"MEGA/SDK Rclone filesystem", // userAgent
		uint(opt.WorkerThreads))      // workerThreadCount
	srv.AddRequestListener(listener)

	// Use HTTPS
	// TODO: Should i check for errors here?
	listenerObj.Reset()
	srv.UseHttpsOnly(opt.UseHTTPS, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		fs.Errorf("mega-native", "Couldn't set UseHttpsOnly %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	// DL Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_DOWNLOAD, opt.DownloadThreads, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		fs.Errorf("mega-native", "Couldn't set download threads %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	// UP Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_UPLOAD, opt.UploadThreads, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		fs.Errorf("mega-native", "Couldn't set upload threads %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	// TODO: Use debug for something
	fs.Logf("mega-native", "Trying log in...")

	listenerObj.Reset()
	srv.Login(opt.User, opt.Pass)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return nil, fmt.Errorf("couldn't login: %d - %s", merr.GetErrorCode(), merr.ToString())
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
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Find the root node and check if it is a file or not
	fs.Logf("mega-native", "Retrieving root node...")
	rootNode := f.API().GetNodeByPath(f.root)

	// This occurs in "move" operations, where the destionation filesystem shouldn't have a "root"
	if rootNode.Swigcptr() == 0 {
		fs.Debugf("mega-native", "Couldn't find root node!")
		return f, nil
	}

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
func getMegaError(listener *MyMegaListener) mega.MegaError {
	megaerr := listener.GetError()
	if megaerr == nil {
		return nil
	}

	return *megaerr
}

// Just a macro
func (f *Fs) API() mega.MegaApi {
	return *f.srv
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
func getTransferListener() (*MyMegaTransferListener, mega.MegaTransferListener) {
	listenerObj := MyMegaTransferListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)
	listenerObj.director = &listener
	return &listenerObj, listener
}

// Permanently deletes a node
func (f *Fs) hardDelete(node mega.MegaNode) error {
	fs.Debugf(f, "hardDelete %s", node.GetName())

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	listenerObj.Reset()
	f.API().Remove(node, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("delete error: %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	return nil
}

// Deletes a node
func (f *Fs) delete(node mega.MegaNode) error {
	fs.Debugf(f, "delete %s", node.GetName())

	if f.opt.HardDelete {
		if err := f.hardDelete(node); err != nil {
			return err
		}
	} else {
		if err := f.moveNode(node, f.API().GetRubbishNode()); err != nil {
			return err
		}
	}

	return nil
}

// Moves a node into a directory node
func (f *Fs) moveNode(node mega.MegaNode, dir mega.MegaNode) error {
	fs.Debugf(f, "moveNode %s -> %s/", node.GetName(), dir.GetName())

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	listenerObj.Reset()
	f.API().MoveNode(node, dir, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("move error: %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	return nil
}

// Rename a node
func (f *Fs) renameNode(node mega.MegaNode, name string) error {
	fs.Debugf(f, "rename %s -> %s", node.GetName(), name)

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	listenerObj.Reset()
	f.API().RenameNode(node, name, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("rename error: %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	return nil
}

// Creates a root if missing
func (f *Fs) rootFix() error {
	if f._rootNode == nil {
		root, rootName := filepath.Split(f.root)
		rootNode := f.API().GetNodeByPath(root)
		if rootNode.Swigcptr() == 0 {
			return fs.ErrorDirNotFound
		}

		listenerObj, listener := getRequestListener()
		defer mega.DeleteDirectorMegaRequestListener(listener)

		// Create folder
		listenerObj.Reset()
		f.API().CreateFolder(rootName, rootNode, listener)
		listenerObj.Wait()

		if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
			return fmt.Errorf("MEGA Mkdir root Error: %d - %s", merr.GetErrorCode(), merr.ToString())
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
func (f *Fs) mkdirParent(path string) (*mega.MegaNode, error) {
	fs.Debugf(f, "mkdirParent %s", path)

	err := f.rootFix()
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

	return f.mkdir(_rootName, &_rootParentNode)
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
			dataCh <- children.Get(i)
		}
	}()

	return dataCh, nil
}

// Make directory and return it
func (f *Fs) mkdir(name string, parent *mega.MegaNode) (*mega.MegaNode, error) {
	fs.Debugf(f, "mkdir %s/%s", (*parent).GetName(), name)

	err := f.rootFix()
	if err != nil {
		return nil, err
	}

	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Create folder
	listenerObj.Reset()
	f.API().CreateFolder(name, parent, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return nil, fmt.Errorf("MEGA Mkdir Error: %d - %s", merr.GetErrorCode(), merr.ToString())
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
	f.rootFix()

	dirNode, err := f.findDir(dir)
	if err != nil {
		return nil, err
	}

	children, err := f.iterChildren(*dirNode)
	if err != nil {
		return nil, err
	}

	// TODO: Some pointer bug that causes same size for all files?
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

	_, err = f.mkdir(srcName, n)
	return err
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "NewObject %s", remote)
	f.rootFix()

	o := &Object{
		fs:     f,
		remote: remote,
	}

	_, err := f.mkdirParent(remote)
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

	if err := f.delete(*n); err != nil {
		return err
	}

	fs.Debugf(f, "Folder deleted OK")
	return nil
}

// ------------------------------------------------------------

// BufferedReaderCloser is a custom type that implements io.ReaderCloser
type BufferedReaderCloser struct {
	buffer     []byte
	bufferSize int
	bufferMu   sync.Mutex
	closed     bool
	paused     bool
	fs         *Fs
	listener   *MyMegaTransferListener
}

// NewBufferedReaderCloser creates a new BufferedReaderCloser with a specified buffer size.
func NewBufferedReaderCloser(bufferSize int) *BufferedReaderCloser {
	return &BufferedReaderCloser{
		buffer:     []byte{},
		bufferSize: bufferSize,
		closed:     false,
		paused:     false,
	}
}

// Check if the buffer size exceeds double the bufferSize
func (b *BufferedReaderCloser) CheckExceeds() {
	if b.paused || len(b.buffer) < b.bufferSize {
		return
	}

	_transfer := b.listener.GetTransfer()
	if _transfer != nil {
		transfer := *_transfer
		if transfer.GetState() == mega.MegaTransferSTATE_ACTIVE {
			if err := b.fs.setPause(transfer, true); err != nil {
				fs.Debugf(b.fs, "Transfer resume")
				b.paused = true
				return
			}
		}
	}
	fs.Debugf(b.fs, "Transfer resume failed")
}

// Check if the buffer size is less or equal to the buffer size
func (b *BufferedReaderCloser) CheckLess() {
	if !b.paused || len(b.buffer) > b.bufferSize/2 {
		return
	}

	_transfer := b.listener.GetTransfer()
	if _transfer != nil {
		transfer := *_transfer
		if transfer.GetState() == mega.MegaTransferSTATE_PAUSED {
			if err := b.fs.setPause(transfer, false); err != nil {
				fs.Debugf(b.fs, "Transfer pause")
				b.paused = false
				return
			}
		}
	}
	fs.Debugf(b.fs, "Transfer pause failed")
}

// Read reads up to len(p) bytes into p.
func (b *BufferedReaderCloser) Read(p []byte) (n int, err error) {
	for len(b.buffer) == 0 && !b.closed {
		time.Sleep(time.Millisecond * 10)
	}

	if b.closed /*&& len(b.buffer) == 0*/ {
		return 0, io.EOF
	}

	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	n = copy(p, b.buffer)
	b.buffer = b.buffer[n:]
	b.CheckLess()

	return n, nil
}

// Close closed the file - MAC errors are reported here
func (b *BufferedReaderCloser) Close() error {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	fs.Debugf(b.fs, "File Close")

	// Ask Mega kindly to stop
	/*transfer := b.listener.GetTransfer()
	if transfer != nil {
		// TODO: Attach listener to this?
		b.fs.API().CancelTransfer(*transfer)
	}*/

	// TODO: Causes crashes?
	//defer mega.DeleteDirectorMegaTransferListener(*b.listener.director)

	/*
		2023/10/05 16:14:21 DEBUG : mega root '/memes': Size video14058428974.mp4
		2023/10/05 16:14:21 DEBUG : mega root '/memes': File Close
		2023/10/05 16:14:21 DEBUG : mega root '/memes': File Close 2
		INFO: Request finished with error -9 - Not found
		zsh: segmentation fault (core dumped)  ./rclone -vvv mount --allow-other --vfs-cache-mode writes test:memes
	*/

	// b.listener.Wait()
	/*merr := b.listener.GetError()
	if merr != nil && (*merr).GetErrorCode() != mega.MegaErrorAPI_OK {
		fs.Debugf(b.fs, "MEGA Transfer error: %d - %s", (*merr).GetErrorCode(), (*merr).ToString())
	}*/

	fs.Debugf(b.fs, "File Close 2")

	b.closed = true
	return nil
}

// Set the pause state of a transfer
func (f *Fs) setPause(signal mega.MegaTransfer, paused bool) error {
	listenerObj, listener := getRequestListener()
	defer mega.DeleteDirectorMegaRequestListener(listener)

	listenerObj.Reset()
	f.API().PauseTransfer(signal, paused, listener)
	listenerObj.Wait()

	if merr := getMegaError(listenerObj); merr != nil && merr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("MEGA SetPause root error: %d - %s", merr.GetErrorCode(), merr.ToString())
	}

	return nil
}

// WriteToBuffer writes data to the buffer.
func (b *BufferedReaderCloser) WriteToBuffer(data []byte) error {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	if b.closed {
		return io.ErrClosedPipe
	}
	b.buffer = append(b.buffer, data...)
	b.CheckExceeds()

	return nil
}

// Signals an EOF
func (b *BufferedReaderCloser) EOF() {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	fs.Debugf(b.fs, "File EOF")

	/*b.listener.Wait()
	merr := b.listener.GetError()
	if merr != nil && (*merr).GetErrorCode() != mega.MegaErrorAPI_OK {
		fs.Debugf(b.fs, "MEGA Transfer error EOF: %d - %s", (*merr).GetErrorCode(), (*merr).ToString())
		b.closed = true
	}*/
	/*b.listener.Wait()
	b.closed = true*/
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Debugf(o.fs, "File Open %s", o.Remote())

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
	if 0 > limit || limit+offset > o.Size() {
		limit = o.Size() - offset
	}

	_node, err := o.getRef()
	if err != nil {
		return nil, err
	}
	node := (*_node).Copy()
	fmt.Printf("NAME %s\n", node.GetName())

	// Create listener
	listenerObj, listener := getTransferListener()

	/* TODO BUG: If mounted read fails
	2023/10/05 16:41:00 DEBUG : mega root '/memes': File Close
	2023/10/05 16:41:00 DEBUG : mega root '/memes': File Close 2
	INFO: Request finished with error -9 - Not found
	segmentation fault (core dumped)  ./rclone -vvv mount --allow-other --vfs-cache-mode writes test:memes
	*/

	// 4MB buffer
	// TODO: Config this?
	reader := NewBufferedReaderCloser(1024 * 1024 * 4)
	listenerObj.out = reader
	reader.fs = o.fs
	reader.listener = listenerObj

	listenerObj.Reset()
	o.fs.API().StartStreaming(node, offset, limit, listener)

	return readers.NewLimitedReadCloser(reader, limit), nil
}

// ------------------------------------------------------------

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (o *Object) Remove(ctx context.Context) error {
	fs.Debugf(o.fs, "Remove %s", o.Remote())

	n, err := o.getRef()
	if err != nil {
		return err
	}

	if !(*n).IsFile() {
		return fs.ErrorIsDir
	}

	if err := o.fs.delete(*n); err != nil {
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

	if err := f.delete(*n); err != nil {
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
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	fs.Debugf(f, "Move %q -> %q", src.Remote(), remote)

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(f, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcNode, err := srcObj.getRef()
	if err != nil {
		return nil, err
	}

	dest, err := f.findObject(remote)
	if err == nil && (*dest).IsFile() {
		fs.Debugf(f, "The destination is an existing file")
		return nil, fs.ErrorIsFile
	}

	destNode, err := f.mkdirParent(remote)
	if err != nil && err != fs.ErrorDirExists {
		fs.Debugf(f, "Destination folder not found")
		return nil, err
	}

	absSrc, _ := f.parsePath(src.Remote())
	absDst, _ := f.parsePath(remote)
	srcPath, srcName := filepath.Split(absSrc)
	dstPath, dstName := filepath.Split(absDst)
	if srcPath != dstPath {
		if err := f.moveNode(*srcNode, *destNode); err != nil {
			return nil, err
		}
	}
	if srcName != dstName {
		if err := f.renameNode(*srcNode, dstName); err != nil {
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
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	dstFs := f
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	// TODO: Test this

	absSrc, _ := srcFs.parsePath(srcRemote)
	absDst, _ := dstFs.parsePath(dstRemote)
	fs.Debugf(f, "DirMove: %s -> %s", absSrc, absDst)

	srcNode, err := srcFs.findDir(srcRemote)
	if err != nil {
		return err
	}

	dstNode, err := dstFs.mkdirParent(dstRemote)
	if err != nil && err != fs.ErrorDirExists {
		return err
	}

	srcPath, srcName := filepath.Split(absSrc)
	destPath, destName := filepath.Split(absDst)
	if srcPath != destPath {
		if err := f.moveNode(*srcNode, *dstNode); err != nil {
			return err
		}
	}
	if srcName != destName {
		if err := f.renameNode(*srcNode, destName); err != nil {
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
			if err := f.moveNode(node, dstNode); err != nil {
				return err
			}

			// TODO: Is this correct
			if err := f.delete(node); err != nil {
				return err
			}
		}

	}

	return nil
}

// ------------------------------------------------------------

// About gets quota information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	fs.Debugf(f, "About")

	f.API().GetAccountDetails()
	(*f.srvListen).Wait()
	defer (*f.srvListen).Reset()

	data := (*f.srvListen).request
	if (*data).GetType() != mega.MegaRequestTYPE_ACCOUNT_DETAILS {
		return nil, fmt.Errorf("something went wrong")
	}
	account_details := (*data).GetMegaAccountDetails()

	usage := &fs.Usage{
		Total: fs.NewUsageValue(account_details.GetStorageMax()),                                    // quota of bytes that can be used
		Used:  fs.NewUsageValue(account_details.GetStorageUsed()),                                   // bytes in use
		Free:  fs.NewUsageValue(account_details.GetStorageMax() - account_details.GetStorageUsed()), // bytes which can be uploaded before reaching the quota
	}
	return usage, nil
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	fs.Debugf(o.fs, "String %s", o.Remote())
	if o == nil {
		return "<nil>"
	}
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

	return o.fs.API().GetCRC(node), nil
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

	_parentNode, err := f.mkdirParent(o.remote)
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
	listenerObj, listener := getTransferListener()
	defer mega.DeleteDirectorMegaTransferListener(listener)

	// Create cancel token
	token := mega.MegaCancelTokenCreateInstance()
	defer mega.DeleteMegaCancelToken(token)

	// Upload
	_, fileName := filepath.Split(o.remote)
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

	// Wait
	listenerObj.Wait()

	// If error
	merr := listenerObj.GetError()
	if merr != nil && (*merr).GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("couldn't upload: %d - %s", (*merr).GetErrorCode(), (*merr).ToString())
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

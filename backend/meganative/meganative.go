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
	fs     *Fs            // what this object is part of
	remote string         // The remote path
	info   *mega.MegaNode // pointer to the mega node
}

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
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)

	// TODO: Generate code at: https://mega.co.nz/#sdk
	srv := mega.NewMegaApi(
		"ht1gUZLZ",                   // appKey
		opt.Cache,                    // basePath
		"MEGA/SDK Rclone filesystem", // userAgent
		uint(opt.WorkerThreads))      // workerThreadCount
	srv.AddRequestListener(listener)

	// Use HTTPS
	listenerObj.Reset()
	srv.UseHttpsOnly(opt.UseHTTPS, listener)
	listenerObj.Wait()

	// DL Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_DOWNLOAD, opt.DownloadThreads, listener)
	listenerObj.Wait()

	// UP Threads
	listenerObj.Reset()
	srv.SetMaxConnections(mega.MegaTransferTYPE_UPLOAD, opt.UploadThreads, listener)
	listenerObj.Wait()

	// TODO: Use debug for something
	fs.Debugf("mega-native", "Trying log in...")

	listenerObj.Reset()
	srv.Login(opt.User, opt.Pass)
	listenerObj.Wait()

	megaerr := *listenerObj.GetError()
	if megaerr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return nil, fmt.Errorf("couldn't login: %s", megaerr.ToString())
	}

	fs.Debugf("mega-native", "Waiting for nodes...")
	for listenerObj.cwd == nil {
		time.Sleep(1 * time.Second)
	}

	root = filepath.Join("/", strings.TrimSuffix(root, "/"))
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		srv:       &srv,
		srvListen: &listenerObj,
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Find the root node and check if it is a file or not
	fs.Debugf("mega-native", "Retrieving root node...")
	rootNode := f.API().GetNodeByPath(root)
	// This occurs in "move" operations, where the destionation filesystem shouldn't have a "root"
	if rootNode.Swigcptr() == 0 {
		fs.Debugf("mega-native", "Couldn't find root node!")
		return f, nil // TODO: fs.ErrorObjectNotFound ?
	}

	f._rootNode = &rootNode
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
	}
	return f, nil
}

// ------------------------------------------------------------
// Support functions

// parsePath parses a mega 'url'
func (f *Fs) parsePath(path string) (string, mega.MegaNode) {
	return strings.Trim(path, "/"), *f._rootNode
}

// Converts any mega unix time to time.Time
func intToTime(num int64) time.Time {
	return time.Unix(num, 0)
}

// Just a macro
func (f *Fs) API() mega.MegaApi {
	return *f.srv
}

// findDir finds the directory rooted from the node passed in
func (f *Fs) findDir(dir string) (*mega.MegaNode, error) {
	//fmt.Printf("HERE7 %s\n", f.parsePath(dir))
	n := f.API().GetNodeByPath(f.parsePath(dir))
	if n.Swigcptr() == 0 {
		fmt.Printf("G1\n")
		return nil, fs.ErrorDirNotFound
	} else if n.Swigcptr() != 0 && n.GetType() == mega.MegaNodeTYPE_FILE {
		fmt.Printf("G2\n")
		return nil, fs.ErrorIsFile
	}
	fmt.Printf("G3\n")
	return &n, nil
}

// findObject looks up the node for the object of the name given
func (f *Fs) findObject(file string) (*mega.MegaNode, error) {
	fmt.Printf("HERE8\n")
	n := f.API().GetNodeByPath(f.parsePath(file))
	if n.Swigcptr() == 0 {
		return nil, fs.ErrorObjectNotFound
	} else if n.Swigcptr() != 0 && n.GetType() != mega.MegaNodeTYPE_FILE {
		return nil, fs.ErrorIsDir
	}
	return &n, nil
}

func (f *Fs) hardDelete(node mega.MegaNode) error {
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)
	defer mega.DeleteDirectorMegaRequestListener(listener)

	f.API().Remove(node, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	megaerr := *listenerObj.GetError()
	if megaerr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("delete error: %s", megaerr.ToString())
	}

	return nil
}

func (f *Fs) delete(node mega.MegaNode) error {
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

func (f *Fs) moveNode(node mega.MegaNode, dir mega.MegaNode) error {
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)
	defer mega.DeleteDirectorMegaRequestListener(listener)

	f.API().MoveNode(node, dir, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	megaerr := *listenerObj.GetError()
	if megaerr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("move error: %s", megaerr.ToString())
	}

	return nil
}

func (f *Fs) renameNode(node mega.MegaNode, name string) error {
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)
	defer mega.DeleteDirectorMegaRequestListener(listener)

	f.API().RenameNode(node, name, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	megaerr := *listenerObj.GetError()
	if megaerr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("rename error: %s", megaerr.ToString())
	}

	return nil
}

// Make parent directory and return it
func (f *Fs) mkdirParent(path string) (*mega.MegaNode, error) {
	path, rootNode := f.parsePath(path)

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

func (f *Fs) iterChildren(node mega.MegaNode) (<-chan mega.MegaNode, error) {
	children := f.API().GetChildren(node)
	if children.Swigcptr() == 0 {
		return nil, fs.ErrorListAborted
	}

	dataCh := make(chan mega.MegaNode)
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
	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)
	defer mega.DeleteDirectorMegaRequestListener(listener)

	// Create folder
	f.API().CreateFolder(name, parent, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	megaerr := *listenerObj.GetError()
	if megaerr.GetErrorCode() != mega.MegaErrorAPI_OK {
		return nil, fmt.Errorf("MEGA Mkdir Error: %s", megaerr.ToString())
	}

	// Find resulting folder
	children, err := f.iterChildren(*parent)
	if err != nil {
		return nil, fmt.Errorf("failed to find children of folder %s", name)
	}

	for node := range children {
		if node.GetName() == name && node.GetType() == mega.MegaNodeTYPE_FOLDER {
			return &node, nil
		}
	}

	return nil, fmt.Errorf("created folder not found")
}

// ------------------------------------------------------------

// List implements fs.Fs.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fmt.Printf("HERE9 %s\n", dir)
	dirNode, err := f.findDir(dir)
	if err != nil {
		fmt.Printf("J1\n")
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
				info:   &node,
			}

			entries = append(entries, o)
		}
	}
	fmt.Printf("J3\n")

	return entries, nil
}

// Mkdir implements fs.Fs.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fmt.Printf("HERE11\n")
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

// NewObject implements fs.Fs.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fmt.Printf("HERE3\n")
	o := &Object{
		fs:     f,
		remote: remote,
	}

	var err error
	n := f.API().GetNodeByPath(f.parsePath(remote))
	if n.Swigcptr() != 0 {
		o.info = &n
	} else {
		err = fs.ErrorObjectNotFound
	}

	return o, err
}

// Precision implements fs.Fs.
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Put implements fs.Fs.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fmt.Printf("HERE11\n")
	existingObj, err := f.findObject(src.Remote())
	if err != nil {
		return f.PutUnchecked(ctx, in, src)
	}

	o := &Object{
		fs:     f,
		remote: src.Remote(),
		info:   existingObj,
	}

	return o, o.Update(ctx, in, src, options...)
}

// Rmdir implements fs.Fs.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	fmt.Printf("PATH2: %s\n", dir)
	n, err := f.findDir(dir)
	if err != nil {
		return err
	}

	if !(*n).IsFolder() {
		return fmt.Errorf("the path isn't a folder")
	}

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
	buffer   []byte
	bufferMu sync.Mutex
	readCh   chan []byte
	closeCh  chan struct{}
	closed   bool
	obj      *Object
	listener *MyMegaTransferListener
}

// NewBufferedReaderCloser creates a new BufferedReaderCloser with a specified buffer size.
func NewBufferedReaderCloser(bufferSize int) *BufferedReaderCloser {
	return &BufferedReaderCloser{
		buffer:  make([]byte, bufferSize),
		readCh:  make(chan []byte),
		closeCh: make(chan struct{}),
		closed:  false,
	}
}

// Read reads data from the buffer.
func (b *BufferedReaderCloser) Read(p []byte) (n int, err error) {
	select {
	case data := <-b.readCh:
		n = copy(p, data)
		return n, nil
	case <-b.closeCh:
		return 0, io.EOF
	}
}

// Close closes the BufferedReaderCloser.
func (b *BufferedReaderCloser) Close() error {
	b.bufferMu.Lock()

	// Ask Mega kindly to stop
	b.obj.fs.API().CancelTransfer(*b.listener.GetTransfer())
	defer mega.DeleteDirectorMegaTransferListener(*b.listener.director)

	if !b.closed {
		close(b.closeCh)
		b.closed = true
	}
	b.bufferMu.Unlock()
	return nil
}

// WriteToBuffer writes data to the buffer.
func (b *BufferedReaderCloser) WriteToBuffer(data []byte) error {
	b.bufferMu.Lock()
	if b.closed {
		b.bufferMu.Unlock()
		return io.ErrClosedPipe
	}
	copy(b.buffer, data)
	b.readCh <- b.buffer
	b.bufferMu.Unlock()
	return nil
}

// Open implements fs.Object.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
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

	// Bigger than file limit
	if 0 > limit || limit+offset > (*o.info).GetSize() {
		limit = (*o.info).GetSize() - offset
	}

	// Create listener
	listenerObj := MyMegaTransferListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)
	listenerObj.director = &listener

	// 1MB buffer
	reader := NewBufferedReaderCloser(1024 * 1024)
	listenerObj.out = reader
	reader.obj = o
	reader.listener = &listenerObj

	// TODO: If listener.notified then its stopped, detect this
	o.fs.API().StartStreaming(*o.info, offset, limit, listener)

	return readers.NewLimitedReadCloser(reader, limit), nil
}

// ------------------------------------------------------------

// Remove implements fs.Object.
func (o *Object) Remove(ctx context.Context) error {
	fmt.Printf("PATH3")
	if o.info == nil {
		return fmt.Errorf("file not found")
	}

	n := *o.info

	if !n.IsFile() {
		return fmt.Errorf("the path isn't a file")
	}

	if err := o.fs.delete(n); err != nil {
		return err
	}

	fs.Debugf(o.fs, "File deleted OK")
	return nil
}

// ------------------------------------------------------------

// Purge deletes all the files in the directory
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	fmt.Printf("PATH71\n")
	n, err := f.findDir(dir)
	if err != nil {
		return err
	}

	if !(*n).IsFolder() {
		return fmt.Errorf("the path isn't a folder")
	}

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
	fmt.Printf("HERE12\n")
	fs.Debugf(f, "Move %q -> %q", src.Remote(), remote)
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(f, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcNode, err := f.findObject(src.Remote())
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
	// Shouldn't be necessary
	srcObj.info = srcNode
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

	absSrc, _ := srcFs.parsePath(srcRemote)
	absDst, _ := dstFs.parsePath(dstRemote)
	fmt.Printf("DirMove: %s -> %s\n", absSrc, absDst)
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

func (f *Fs) write(ctx context.Context, dstObj *Object, in io.Reader, src fs.ObjectInfo, ignoreExist bool) (*Object, error) {
	fmt.Printf("HERE1\n")
	node, err := f.findObject(dstObj.remote)
	if err != nil && !ignoreExist {
		return dstObj, err
	}

	pnode, err := f.mkdirParent(dstObj.remote)
	if err != nil && err != fs.ErrorDirExists {
		fs.Debugf(f, "Parent folder creation failed")
		return nil, err
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "mega*.tmp")
	if err != nil {
		return dstObj, fmt.Errorf("failed to create temporary file")
	}
	defer os.Remove(tempFile.Name())

	// Unlike downloads, there is no upload bytes interface
	// It must be a file, so we create a temporary one
	_, err = io.Copy(tempFile, in)
	if err != nil {
		return dstObj, fmt.Errorf("failed to write temporary file")
	}

	listenerObj := MyMegaTransferListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)
	defer mega.DeleteDirectorMegaTransferListener(listener)

	token := mega.MegaCancelTokenCreateInstance()
	defer mega.DeleteMegaCancelToken(token)

	_, fileName := filepath.Split(dstObj.remote)
	// Somebody contact MEGA, ask them for a upload chunks interface :P
	f.API().StartUpload(
		tempFile.Name(),         //Localpath
		*pnode,                  //Directory
		fileName,                //Filename
		src.ModTime(ctx).Unix(), //Modification time
		"",                      // Temporary directory
		false,                   // Temporary source
		false,                   // Priority
		token,                   // Cancel token
		listener,                //Listener
	)
	listenerObj.Wait()
	defer listenerObj.Reset()

	// Delete old node
	err = nil
	if node != nil {
		// TODO: Is this correct and allows versioning, or should i move to trash?
		err = f.hardDelete(*node)
	}

	return dstObj, err
}

// MergeDirs merges the contents of all the directories passed
// in into the first one and rmdirs the other directories.
func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) error {
	fmt.Printf("PATH70")

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

			// TODO: Is this correct and allows versioning, or should i move to trash?
			if err := f.hardDelete(node); err != nil {
				return err
			}
		}

	}

	return nil
}

// ------------------------------------------------------------

// About implements fs.Abouter.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
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

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	fmt.Printf("PATH71")
	node := f.API().GetNodeByPath(f.parsePath(remote))
	if node.Swigcptr() == 0 {
		return "", fmt.Errorf("object not found")
	}

	var err error
	link := node.GetPublicLink(true)
	if link == "" {
		err = fmt.Errorf("non-exported file")
	}

	return link, err
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	dstObj := &Object{
		fs:     f,
		remote: src.Remote(),
	}

	return f.write(ctx, dstObj, in, src, false)
}

// Update implements fs.Object.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fmt.Printf("HERE5\n")
	// TODO: Test this
	_, err := (*o.fs).write(ctx, o, in, src, false)
	return err
}

// ModTime implements fs.DirEntry.
func (o *Object) ModTime(context.Context) time.Time {
	fmt.Printf("test2\n")
	if o.info != nil {
		return intToTime((*o.info).GetModificationTime())
	}
	return time.Unix(0, 0)
}

// Remote implements fs.DirEntry.
func (o *Object) Remote() string {
	return o.remote
}

// Size implements fs.DirEntry.
func (o *Object) Size() int64 {
	fmt.Printf("HERE4\n")
	if o.info != nil {
		return (*o.info).GetSize()
	}
	return -1
}

// String implements fs.DirEntry.
func (o *Object) String() string {
	fmt.Printf("test3\n")
	if o == nil {
		return "<nil>"
	}
	path, _ := o.fs.parsePath(o.remote)
	return path
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash implements fs.Object.
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	// TODO: Is reportedly a "Base64-encoded CRC of the file. The CRC of a file is a hash of its contents"
	// But i cant figure it out
	return o.fs.API().GetCRC(*o.info), nil
}

// SetModTime implements fs.Object.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable implements fs.Object.
func (o *Object) Storable() bool {
	return true
}

// DirCacheFlush implements fs.DirCacheFlusher.
func (f *Fs) DirCacheFlush() {
	// TODO: Catchup?
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return (*o.info).GetBase64Handle()
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

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	// TODO: Mega supports CRC, but im unsure of the format
	return hash.Set(hash.None)
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

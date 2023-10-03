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
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/readers"
)

// STRONG WARNING:
// This is from SWIG bindings of a C++ project
// There is insane amounts of pointer fuckery
// The code below in almost no way resembles normal golang code
// So for your safety, if you are dainty about golang code
// Do not scroll further down

/*
 * See megaapi.h for source documentation
 * Read MEGAdokan.cpp for a full fs driver, use this as main source, but also a little wrong download
 *
 * Read megacli.php for a full client, but with some wrong download/upload args
 * Read megafuse.cpp for nice c++ code
 * Read https://mega.io/developers
 *
 * TODO: Find out about the state files that are generated
 */

/*
 * The path separator character is '/'
 * The Root node is /
 * The Vault root node is //in/
 * The Rubbish root node is //bin/
 *
 * Paths with names containing '/', '\' or ':' aren't compatible
 * with this function.
 */

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	eventWaitTime = 500 * time.Millisecond
	decayConstant = 2 // bigger for slower decay, exponential
)

// Options defines the configuration for this backend
type Options struct {
	User       string               `config:"user"`
	Pass       string               `config:"pass"`
	Cache      string               `config:"cache"`
	Debug      bool                 `config:"debug"`
	HardDelete bool                 `config:"hard_delete"`
	UseHTTPS   bool                 `config:"use_https"`
	Enc        encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote mega
type Fs struct {
	name      string          // name of this remote
	root      string          // the path we are working on
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

// ID implements fs.IDer.
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

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "mega_native",
		Description: "Mega Native",
		NewFs:       NewFs,
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
			Name:     "cache",
			Help:     "MEGA State cache folder.",
			Default:  "",
			Required: false,
			Advanced: true,
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
	director := mega.NewDirectorMegaListener(&listenerObj)

	// TODO: Generate code at: https://mega.co.nz/#sdk
	srv := mega.NewMegaApi(
		"ht1gUZLZ",                   // appKey
		"megacache",                  // basePath
		"MEGA/SDK Rclone filesystem", // userAgent
		uint(4))                      // workerThreadCount
	srv.AddListener(director)

	srv.UseHttpsOnly(opt.UseHTTPS)
	listenerObj.Wait()
	listenerObj.Reset()

	fs.Debugf("mega-native", "Trying log in...")
	srv.Login(opt.User, opt.Pass)
	listenerObj.Wait()
	if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
		return nil, fmt.Errorf("couldn't login: %w", err)
	}
	listenerObj.Reset()

	fs.Debugf("mega-native", "Waiting for nodes...")
	for listenerObj.cwd == nil {
		time.Sleep(1 * time.Second)
	}

	root = filepath.Join("/", strings.Trim(root, "/"))
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
	if rootNode.Swigcptr() == 0 {
		return nil, fmt.Errorf("couldn't find root node")
	}

	/*switch rootNode.GetType() {
	case mega.MegaNodeTYPE_FOLDER:
		// root node found and is a directory
	case mega.MegaNodeTYPE_FILE:
		// root node is a file so point to parent directory
		root = path.Dir(root)
		if root == "." {
			root = ""
		}
		f.root = root
	}*/
	fs.Debugf("mega-native", "Ready!")
	return f, nil
}

// ------------------------------------------------------------

// parsePath parses a mega 'url'
func (f *Fs) parsePath(path string) (root string) {
	fmt.Printf("HERE10 %s\n", path)
	root = filepath.Join(f.root, strings.Trim(path, "/"))
	fmt.Printf("HERE10.1 %s\n", root)
	return
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
	fmt.Printf("HERE7\n")
	n := f.API().GetNodeByPath(f.parsePath(dir))
	if n.Swigcptr() == 0 {
		return nil, fs.ErrorDirNotFound
	} else if n.Swigcptr() != 0 && n.GetType() == mega.MegaNodeTYPE_FILE {
		return nil, fs.ErrorIsFile
	}
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

	f.API().Remove(node, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("error deleting folder")
	}

	return nil
}

// ------------------------------------------------------------

// List implements fs.Fs.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fmt.Printf("HERE9\n")
	_dirNode, err := f.findDir(dir)
	if err != nil {
		return nil, err
	}
	dirNode := *_dirNode

	children := f.API().GetChildren(dirNode)
	if children.Swigcptr() == 0 {
		return nil, fmt.Errorf("couldn't list files")
	}

	for i := 0; i < children.Size(); i++ {
		node := children.Get(i)
		remote := path.Join(dir, f.opt.Enc.ToStandardName(node.GetName()))
		switch node.GetType() {
		case mega.MegaNodeTYPE_FOLDER:
			modTime := intToTime(node.GetCreationTime())
			d := fs.NewDir(remote, modTime).SetID(node.GetBase64Handle()).SetParentID(dirNode.GetBase64Handle())
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

	return entries, nil
}

// Mkdir implements fs.Fs.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fmt.Printf("HERE11\n")
	_, err := f.findDir(dir)
	if err == nil {
		return fmt.Errorf("Mkdir failed: %w", err)
	}

	index := strings.LastIndex(dir, "/")
	parentDir := dir[:index+1]
	fileName := dir[index+1:]
	n, err := f.findDir(parentDir)
	if err != nil || (*n).IsFile() {
		return err
	}

	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)

	f.API().CreateFolder(fileName, *n, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("error creating folder")
	}

	fs.Debugf(f, "Folder created OK")
	return nil
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
	// TODO: write literally does all of this, move this logic out of here
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
	// TODO: Test this
	n, err := f.findDir(dir)
	if err != nil {
		return err
	}

	if !(*n).IsFolder() {
		return fmt.Errorf("the path isn't a folder")
	}

	if c := (*n).GetChildren(); c.Swigcptr() != 0 || c.Size() > 0 {
		return fmt.Errorf("folder not empty")
	}

	if f.opt.HardDelete {
		if err := f.hardDelete(*n); err != nil {
			return err
		}
	} else {
		// TODO: Is this correct?
		f.API().MoveNode(*n, f.API().GetRubbishNode())
	}

	fs.Debugf(f, "Folder deleted OK")
	return nil
}

// ------------------------------------------------------------

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
	return o.remote
}

// ------------------------------------------------------------

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

type openObject struct {
	*readers.LimitedReadCloser
	o      *Object
	d      *MyMegaTransferListener
	closed bool // Custom field to track if the reader is closed
}

// TODO: Do i need read, or will the builtin wait until chunks (it might not)
// TODO: Folder/file mess bug

// Close custom Close function
func (lrc *openObject) Close() error {
	if lrc.closed {
		return nil // Already closed
	}
	lrc.o.fs.API().CancelTransfer(*lrc.d.GetTransfer())
	lrc.closed = true
	return lrc.Closer.Close()
}

// Open implements fs.Object.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// TODO: Test this (do we need io.SectionReader?)
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
	if 0 > limit || limit+offset > (*o.info).GetSize() {
		limit = (*o.info).GetSize() - offset
	}

	listenerObj := MyMegaTransferListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)

	buf := bytes.NewBufferString("")
	listenerObj.out = buf

	lrc := &openObject{
		o: o,
		d: &listenerObj,
	}
	lrc.R = buf

	o.fs.API().StartStreaming(*o.info, offset, limit, listener)

	return readers.NewLimitedReadCloser(lrc, limit), nil
}

// Remove implements fs.Object.
func (o *Object) Remove(ctx context.Context) error {
	// TODO: Test this
	// TODO: BROKEN (i think)
	/*
		2023/10/02 16:27:17 DEBUG : fs cache: renaming cache item "test:test.txt" to be canonical "test:/test.txt"
		2023/10/02 16:27:17 ERROR : Attempt 1/3 failed with 1 errors and: test:test.txt is a directory or doesn't exist: object not found
		2023/10/02 16:27:17 ERROR : Attempt 2/3 failed with 1 errors and: test:test.txt is a directory or doesn't exist: object not found
		2023/10/02 16:27:17 ERROR : Attempt 3/3 failed with 1 errors and: test:test.txt is a directory or doesn't exist: object not found
		PATH3 isnt printed
	*/
	fmt.Printf("PATH3")
	if o.info == nil {
		return fmt.Errorf("file not found")
	}

	n := *o.info

	if !n.IsFile() {
		return fmt.Errorf("the path isn't a file")
	}

	if o.fs.opt.HardDelete {
		if err := o.fs.hardDelete(n); err != nil {
			return err
		}
	} else {
		// TODO: Is this correct?
		o.fs.API().MoveNode(n, o.fs.API().GetRubbishNode())
	}

	fs.Debugf(o.fs, "File deleted OK")
	return nil
}

// Update implements fs.Object.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fmt.Printf("HERE5\n")
	// TODO: Test this
	_, err := (*o.fs).write(ctx, o, in, src, false)
	return err
}

// SetModTime implements fs.Object.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable implements fs.Object.
func (o *Object) Storable() bool {
	return true
}

// ------------------------------------------------------------

// Purge deletes all the files in the directory
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return fs.ErrorCantPurge
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
	dstFs := f

	fs.Debugf(f, "Move %q -> %q", src.Remote(), remote)
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(f, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	source, err := f.findObject(srcObj.remote)
	if err != nil {
		fs.Debugf(f, "Source not found")
		return nil, fs.ErrorObjectNotFound
	}

	dest, err := f.findObject(remote)
	if err == nil {
		fs.Debugf(f, "The destination is an existing file")
		return nil, fs.ErrorCantMove
	}

	idx := strings.LastIndex(remote, "/")
	destPath := remote[:idx]
	dest, err = f.findDir(destPath)
	if err != nil {
		fs.Debugf(f, "Destination folder not found")
		return nil, fs.ErrorCantMove
	}

	// TODO: Fails because root node doesnt exist
	// We should keep the root to the dir of the folder
	// and somehow fix that we need to keep the original root for move operations
	// that will give "" otherwise, so we need root + remote at all times?
	// TODO: DEBUG
	return nil, fs.ErrorCantMove

	// TODO: If move
	if true {
		listenerObj := MyMegaListener{}
		listenerObj.cv = sync.NewCond(&listenerObj.m)
		listener := mega.NewDirectorMegaRequestListener(&listenerObj)

		f.API().MoveNode(*source, *dest, &listener)
		listenerObj.Wait()
		defer listenerObj.Reset()

		if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
			fs.Debugf(f, "Move failed")
			return nil, fs.ErrorCantMove
		}
	}
	// TODO: If rename
	if false {
		listenerObj := MyMegaListener{}
		listenerObj.cv = sync.NewCond(&listenerObj.m)
		listener := mega.NewDirectorMegaRequestListener(&listenerObj)

		f.API().RenameNode(*source, "test", &listener)
		listenerObj.Wait()
		defer listenerObj.Reset()

		if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
			fs.Debugf(f, "Move failed")
			return nil, fs.ErrorCantMove
		}
	}

	dstObj := &Object{
		fs:     dstFs,
		remote: remote,
		info:   srcObj.info,
	}

	return dstObj, fmt.Errorf("unimplemented")
}

func (f *Fs) write(ctx context.Context, dstObj *Object, in io.Reader, src fs.ObjectInfo, ignore_exist bool) (*Object, error) {
	fmt.Printf("HERE1\n")
	node, err := f.findObject(dstObj.remote)
	if err != nil && !ignore_exist {
		return dstObj, err
	}

	dir := f.parsePath(dstObj.remote)
	index := strings.LastIndex(dir, "/")
	parentDir := dir[:index+1]
	fileName := dir[index+1:]
	pnode, err := f.findObject(parentDir)
	if err != nil {
		return dstObj, fmt.Errorf("failed to find parent folder")
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

	token := mega.MegaCancelTokenCreateInstance()
	defer mega.DeleteMegaCancelToken(token)

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
		err = f.hardDelete(*node)
	}

	return dstObj, err
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	dstObj := &Object{
		fs:     f,
		remote: src.Remote(),
	}

	return f.write(ctx, dstObj, in, src, false)
}

func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	fmt.Printf("PATH69")
	// TODO: .
	return fs.ErrorNotImplemented
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

// MergeDirs implements fs.MergeDirser.
func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) error {
	fmt.Printf("PATH70")
	// TODO: .
	return fs.ErrorNotImplemented
}

// PublicLink implements fs.PublicLinker.
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

// DirCacheFlush implements fs.DirCacheFlusher.
func (f *Fs) DirCacheFlush() {}

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

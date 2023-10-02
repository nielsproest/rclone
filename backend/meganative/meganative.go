package meganative

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
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
	"github.com/rclone/rclone/lib/pacer"
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

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	eventWaitTime = 500 * time.Millisecond
	decayConstant = 2 // bigger for slower decay, exponential
)

// Options defines the configuration for this backend
type Options struct {
	User     string               `config:"user"`
	Pass     string               `config:"pass"`
	Debug    bool                 `config:"debug"`
	UseHTTPS bool                 `config:"use_https"`
	Enc      encoder.MultiEncoder `config:"encoding"`
	// TODO: Hard-delete and the rest
}

// Fs represents a remote mega
type Fs struct {
	name      string        // name of this remote
	root      string        // the path we are working on
	opt       Options       // parsed config options
	features  *fs.Features  // optional features
	srv       *mega.MegaApi // the connection to the server
	srvListen *MyMegaListener
	pacer     *fs.Pacer // pacer for API calls
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
	return (*o.info).GetBase64Key()
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
			Name: "debug",
			Help: `Output more debug from Mega.

If this flag is set (along with -vv) it will print further debugging
information from the mega backend.`,
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
	// TODO: Do i need this reference?
	listenerObj.director = &director

	// TODO: Generate code at: https://mega.co.nz/#sdk
	srv := mega.NewMegaApi("ht1gUZLZ", "", "MEGA/SDK Rclone filesystem")
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

	root = parsePath(root)
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		srv:       &srv,
		srvListen: &listenerObj,
		pacer:     fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Find the root node and check if it is a file or not
	fs.Debugf("mega-native", "Retrieving root node...")
	rootNode := srv.GetRootNode()
	if rootNode.Swigcptr() == 0 {
		return nil, fmt.Errorf("couldn't find root node")
	}

	// TODO: Create folder if missing (or dont?)
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
		return f, err
	}
	fs.Debugf("mega-native", "Ready!")

	return f, nil
}

// ------------------------------------------------------------

// parsePath parses a mega 'url'
// With mega-sdk, the root is ALWAYS /
func parsePath(path string) (root string) {
	root = path
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return
}

func (f *Fs) ParsePath(dir string) string {
	dir = strings.TrimSuffix(dir, "/")

	if f.root == "/" {
		return fmt.Sprintf("%s%s", f.root, dir)
	}

	return fmt.Sprintf("%s/%s", f.root, dir)
}

// Converts any mega unix time to time.Time
func intToTime(num int64) time.Time {
	return time.Unix(num, 0)
}

// Creates a unique id for each node
func nodeToUnique(node *mega.MegaNode) string {
	return strconv.FormatInt((*node).GetHandle(), 10)
}

// Parses the unique id
func (o *Object) Unique() int64 {
	val, _ := strconv.ParseInt(o.ID(), 10, 64)
	return val
}
func (o *Object) FixPath() string {
	return o.fs.ParsePath(o.remote)
}

// ------------------------------------------------------------

// List implements fs.Fs.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dir = parsePath(dir)

	dirNode := (*f.srv).GetNodeByPath(dir)
	if dirNode.Swigcptr() == 0 {
		return nil, fmt.Errorf("couldn't find path")
	}

	children := (*f.srv).GetChildren(dirNode)
	if children.Swigcptr() == 0 {
		return nil, fmt.Errorf("couldn't list files")
	}

	for i := 0; i < children.Size(); i++ {
		node := children.Get(i)

		remote := path.Join(dir, f.opt.Enc.ToStandardName(node.GetName()))
		// While MEGA-SDK explicitly needs / as thats the root
		// Normal filesystems dont like that
		remote = strings.TrimPrefix(remote, "/")

		switch node.GetType() {
		case mega.MegaNodeTYPE_FOLDER:
			modTime := intToTime(node.GetModificationTime())
			d := fs.NewDir(remote, modTime).SetID(nodeToUnique(&node)).SetParentID(nodeToUnique(&dirNode))
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
	dir = f.ParsePath(dir)

	n := (*f.srv).GetNodeByPath(dir)
	if n.Swigcptr() != 0 {
		return fmt.Errorf("path already exists")
	}

	index := strings.LastIndex(dir, "/")
	parentDir := dir[:index+1]
	fileName := dir[index+1:]
	n = (*f.srv).GetNodeByPath(parentDir)
	if n.Swigcptr() == 0 || n.IsFile() {
		return fmt.Errorf("parent folder not found")
	}

	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)

	(*f.srv).CreateFolder(fileName, n, listener)
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
	o := &Object{
		fs:     f,
		remote: remote,
	}

	n := (*f.srv).GetNodeByPath(o.FixPath())
	if n.Swigcptr() == 0 {
		return o, fs.ErrorObjectNotFound
	}
	if !n.IsFile() {
		return o, fs.ErrorIsDir
	}

	o.info = &n
	return o, nil
}

// Precision implements fs.Fs.
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Put implements fs.Fs.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existingObj := (*f.srv).GetNodeByPath(f.ParsePath(src.Remote()))
	if existingObj.Swigcptr() == 0 {
		return f.PutUnchecked(ctx, in, src)
	}

	o := &Object{
		fs:     f,
		remote: src.Remote(),
		info:   &existingObj,
	}

	return o, o.Update(ctx, in, src, options...)
}

// Rmdir implements fs.Fs.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dir = f.ParsePath(dir)

	// TODO: Test this
	n := (*f.srv).GetNodeByPath(dir)
	if n.Swigcptr() == 0 {
		return fmt.Errorf("folder not found")
	}

	if !n.IsFolder() {
		return fmt.Errorf("the path isn't a folder")
	}

	if c := n.GetChildren(); c.Swigcptr() != 0 || c.Size() > 0 {
		return fmt.Errorf("folder not empty")
	}

	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)

	(*f.srv).Remove(n, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("error deleting folder")
	}

	fs.Debugf(f, "Folder deleted OK")
	return nil
}

// ------------------------------------------------------------

// ModTime implements fs.DirEntry.
func (o *Object) ModTime(context.Context) time.Time {
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
	if o.info != nil {
		return (*o.info).GetSize()
	}
	return 0
}

// String implements fs.DirEntry.
func (o *Object) String() string {
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
	return nodeToUnique(o.info), nil
}

// Open implements fs.Object.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// TODO: .
	return nil, fmt.Errorf("unimplemented")
}

// Remove implements fs.Object.
func (o *Object) Remove(ctx context.Context) error {
	// TODO: Test this
	if o.info == nil {
		return fmt.Errorf("file not found")
	}

	n := *o.info

	if !n.IsFile() {
		return fmt.Errorf("the path isn't a file")
	}

	listenerObj := MyMegaListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaRequestListener(&listenerObj)

	(*o.fs.srv).Remove(n, listener)
	listenerObj.Wait()
	defer listenerObj.Reset()

	if (*listenerObj.GetError()).GetErrorCode() != mega.MegaErrorAPI_OK {
		return fmt.Errorf("error deleting file")
	}

	fs.Debugf(o.fs, "File deleted OK")
	return nil
}

// SetModTime implements fs.Object.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable implements fs.Object.
func (o *Object) Storable() bool {
	// TODO: .
	return true
}

// Update implements fs.Object.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// TODO: .
	fmt.Printf("what\n")
	return fmt.Errorf("unimplemented")
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

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	dstFs := f

	//log.Printf("Move %q -> %q", src.Remote(), remote)
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	//source := (*f.srv).GetNodeByHandle()
	// TODO: .

	dstObj := &Object{
		fs:     dstFs,
		remote: remote,
		info:   srcObj.info,
	}

	return dstObj, fmt.Errorf("unimplemented")
}

/*
2023/10/02 13:50:04 DEBUG : notes.txt: Need to transfer - File not found at Destination
2023/10/02 13:50:04 ERROR : notes.txt: corrupted on transfer: sizes differ 1497 vs 0
2023/10/02 13:50:04 INFO  : notes.txt: Removing failed copy
2023/10/02 13:50:04 INFO  : notes.txt: Failed to remove failed copy: file not found
2023/10/02 13:50:04 ERROR : Attempt 1/3 failed with 1 errors and: corrupted on transfer: sizes differ 1497 vs 0
2023/10/02 13:50:04 DEBUG : notes.txt: Size and modification time the same (differ by -183.044725ms, within tolerance 1s)
2023/10/02 13:50:04 DEBUG : notes.txt: Unchanged skipping
2023/10/02 13:50:04 ERROR : Attempt 2/3 succeeded
TODO: Why?
*/
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	dstObj := &Object{
		fs:     f,
		remote: src.Remote(),
	}

	dir := dstObj.FixPath()
	node := (*f.srv).GetNodeByPath(dir)
	if node.Swigcptr() != 0 {
		return dstObj, fmt.Errorf("file exists")
	}

	index := strings.LastIndex(dir, "/")
	parentDir := dir[:index+1]
	fileName := dir[index+1:]
	pnode := (*f.srv).GetNodeByPath(parentDir)
	if node.Swigcptr() != 0 {
		return dstObj, fmt.Errorf("failed to find parent folder")
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "mega*.tmp")
	if err != nil {
		return dstObj, fmt.Errorf("failed to create temporary file")
	}

	_, err = io.Copy(tempFile, in)
	if err != nil {
		return dstObj, fmt.Errorf("failed to write temporary file")
	}

	listenerObj := MyMegaTransferListener{}
	listenerObj.cv = sync.NewCond(&listenerObj.m)
	listener := mega.NewDirectorMegaTransferListener(&listenerObj)

	token := mega.MegaCancelTokenCreateInstance()

	(*f.srv).StartUpload(
		tempFile.Name(),         //Localpath
		pnode,                   //Directory
		fileName,                //Filename
		src.ModTime(ctx).Unix(), //Modification time
		"",                      // Temporary directory
		true,                    // Temporary source
		false,                   // Priority
		token,                   // Cancel token
		listener,                //Listener
	)
	listenerObj.Wait()
	defer listenerObj.Reset()

	return dstObj, nil
}

func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	// TODO: .
	return fmt.Errorf("unimplemented")
}

// ------------------------------------------------------------

// About implements fs.Abouter.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	(*f.srv).GetAccountDetails()
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
	// TODO: .
	return fmt.Errorf("unimplemented")
}

// PublicLink implements fs.PublicLinker.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	// TODO: .
	return "", fmt.Errorf("unimplemented")
}

// DirCacheFlush implements fs.DirCacheFlusher.
func (f *Fs) DirCacheFlush() {
	// (*f.srv).Catchup()
	// TODO: Should i wait for this?
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

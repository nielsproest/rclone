package meganative

import (
	mega "rclone/megasdk"
)

type MyMegaLogListener struct {
	mega.SwigDirector_MegaLogger
}

// Package safename 校验可用作文件名的标识符（taskID、outbox 消息 ID 等）。
//
// 文件后端在写入前校验，非法即报错（设计 §5.1 不变量 4）。
// 校验规则取各主流平台的交集：非空、非 "." / ".."、不含路径分隔符、
// Windows 保留字符与控制字符。
package safename

import (
	"fmt"
	"strings"
	"unicode"
)

const reserved = `\/:*?"<>|`

// Validate 校验 name 是否为文件系统安全字符串，非法时返回描述原因的错误。
func Validate(name string) error {
	if name == "" {
		return fmt.Errorf("safename: empty name")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("safename: %q is a relative path element", name)
	}
	for _, r := range name {
		if unicode.IsControl(r) || strings.ContainsRune(reserved, r) {
			return fmt.Errorf("safename: %q contains reserved character %q", name, r)
		}
	}
	return nil
}

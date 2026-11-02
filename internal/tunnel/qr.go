package tunnel

import (
	"fmt"
	"strings"

	"rsc.io/qr"
)

// QRText draws text as a QR code with half blocks, for a terminal with a
// dark background: two rows of modules a line, with a quiet border.
func QRText(text string) (string, error) {
	// Low error correction keeps the code small enough for a terminal: it
	// is read from a screen, not a worn print.
	code, err := qr.Encode(text, qr.L)
	if err != nil {
		return "", err
	}
	const quiet = 2
	size := code.Size
	// light reports whether a module is light (or outside the code).
	light := func(x, y int) bool {
		if x < 0 || y < 0 || x >= size || y >= size {
			return true
		}
		return !code.Black(x, y)
	}
	var b strings.Builder
	for y := -quiet; y < size+quiet; y += 2 {
		for x := -quiet; x < size+quiet; x++ {
			top, bottom := light(x, y), light(x, y+1)
			switch {
			case top && bottom:
				b.WriteString("█")
			case top:
				b.WriteString("▀")
			case bottom:
				b.WriteString("▄")
			default:
				b.WriteString(" ")
			}
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}

// QRSVG draws text as a QR code in SVG, black on white, a square module a
// unit.
func QRSVG(text string) (string, error) {
	code, err := qr.Encode(text, qr.M)
	if err != nil {
		return "", err
	}
	const quiet = 4
	size := code.Size + 2*quiet
	var path strings.Builder
	for y := 0; y < code.Size; y++ {
		for x := 0; x < code.Size; x++ {
			if code.Black(x, y) {
				fmt.Fprintf(&path, "M%d %dh1v1h-1z", x+quiet, y+quiet)
			}
		}
	}
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" shape-rendering="crispEdges"><rect width="%d" height="%d" fill="#fff"/><path d="%s" fill="#000"/></svg>`,
		size, size, size, size, path.String()), nil
}

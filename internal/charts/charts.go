package charts

import (
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"strings"
)

type Color = color.RGBA

var (
	White = color.RGBA{255, 255, 255, 255}
	Black = color.RGBA{0, 0, 0, 255}
	Gray  = color.RGBA{180, 180, 180, 255}
)

func MustHex(hex string) color.RGBA {
	c, err := Hex(hex)
	if err != nil {
		panic(err)
	}
	return c
}

func Hex(hex string) (color.RGBA, error) {
	hex = strings.TrimPrefix(strings.TrimSpace(hex), "#")
	if len(hex) != 6 {
		return color.RGBA{}, fmt.Errorf("invalid hex color: %q", hex)
	}
	var rgb [3]uint8
	for i := 0; i < 3; i++ {
		var v uint64
		_, err := fmt.Sscanf(hex[i*2:(i+1)*2], "%02x", &v)
		if err != nil {
			return color.RGBA{}, err
		}
		rgb[i] = uint8(v)
	}
	return color.RGBA{rgb[0], rgb[1], rgb[2], 255}, nil
}

func lighten(c color.RGBA, factor float64) color.RGBA {
	clamp := func(v float64) uint8 {
		if v < 0 {
			v = 0
		}
		if v > 255 {
			v = 255
		}
		return uint8(v)
	}
	return color.RGBA{
		R: clamp(float64(c.R) + (255-float64(c.R))*factor),
		G: clamp(float64(c.G) + (255-float64(c.G))*factor),
		B: clamp(float64(c.B) + (255-float64(c.B))*factor),
		A: 255,
	}
}

func darken(c color.RGBA, factor float64) color.RGBA {
	clamp := func(v float64) uint8 {
		if v < 0 {
			v = 0
		}
		if v > 255 {
			v = 255
		}
		return uint8(v)
	}
	return color.RGBA{
		R: clamp(float64(c.R) * (1 - factor)),
		G: clamp(float64(c.G) * (1 - factor)),
		B: clamp(float64(c.B) * (1 - factor)),
		A: 255,
	}
}

func fillRect(img *image.RGBA, x0, y0, x1, y1 int, c color.Color) {
	draw.Draw(img, image.Rect(x0, y0, x1, y1), &image.Uniform{c}, image.Point{}, draw.Src)
}

func setPixel(img *image.RGBA, x, y int, c color.Color) {
	if (image.Point{X: x, Y: y}).In(img.Bounds()) {
		img.Set(x, y, c)
	}
}

func drawLine(img *image.RGBA, x0, y0, x1, y1 int, c color.Color) {
	dx := int(math.Abs(float64(x1 - x0)))
	sx := -1
	if x0 < x1 {
		sx = 1
	}
	dy := -int(math.Abs(float64(y1 - y0)))
	sy := -1
	if y0 < y1 {
		sy = 1
	}
	err := dx + dy
	for {
		setPixel(img, x0, y0, c)
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := 2 * err
		if e2 >= dy {
			err += dy
			x0 += sx
		}
		if e2 <= dx {
			err += dx
			y0 += sy
		}
	}
}

func drawPolyline(img *image.RGBA, pts []image.Point, c color.Color) {
	for i := 1; i < len(pts); i++ {
		drawLine(img, pts[i-1].X, pts[i-1].Y, pts[i].X, pts[i].Y, c)
	}
}

func savePNG(path string, img image.Image) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

func cumulative(values []int64) []int64 {
	out := make([]int64, len(values))
	var sum int64
	for i, v := range values {
		sum += v
		out[i] = sum
	}
	return out
}

func maxInt64(values []int64) int64 {
	var max int64
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	if max <= 0 {
		return 1
	}
	return max
}

func simplifyXLabels(xLabels []string, maxLabels int) []string {
	n := len(xLabels)
	if maxLabels <= 0 || n <= maxLabels {
		out := make([]string, len(xLabels))
		copy(out, xLabels)
		return out
	}
	step := int(math.Ceil(float64(n) / float64(maxLabels)))
	out := make([]string, n)
	for i, label := range xLabels {
		if i%step == 0 || i == n-1 {
			out[i] = label
		}
	}
	return out
}

func formatInt(v int64) string {
	s := fmt.Sprintf("%d", v)
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	if len(s) <= 3 {
		if negative {
			return "-" + s
		}
		return s
	}
	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	b.WriteString(s[:rem])
	for i := rem; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func truncateLabel(s string, max int) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	if max <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func PlotSeriesWithCumulative(title string, xLabels []string, values []int64, path string, barColor color.RGBA, maxLabels int) error {
	width, height := 1200, 500
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	fillRect(img, 0, 0, width, height, White)

	marginLeft, marginRight := 80, 80
	marginTop, marginBottom := 40, 80
	plotX0, plotY0 := marginLeft, marginTop
	plotX1, plotY1 := width-marginRight, height-marginBottom
	plotW, plotH := plotX1-plotX0, plotY1-plotY0

	fillRect(img, plotX0, plotY0, plotX1, plotY1, color.RGBA{248, 248, 248, 255})
	for i := 0; i <= 5; i++ {
		y := plotY1 - int(float64(plotH)*float64(i)/5.0)
		drawLine(img, plotX0, y, plotX1, y, Gray)
	}
	drawLine(img, plotX0, plotY0, plotX0, plotY1, Black)
	drawLine(img, plotX0, plotY1, plotX1, plotY1, Black)
	drawString(img, 20, 15, title, 2, Black)
	drawString(img, 20, 35, "NEW", 1, Black)
	drawString(img, width-60, 35, "CUM", 1, Black)

	if len(values) == 0 {
		drawString(img, plotX0+20, plotY0+20, "NO DATA", 2, Black)
		return savePNG(path, img)
	}

	cumValues := cumulative(values)
	maxBar := maxInt64(values)
	maxCum := maxInt64(cumValues)
	slotW := float64(plotW) / float64(len(values))
	barW := int(slotW * 0.72)
	if barW < 1 {
		barW = 1
	}
	linePts := make([]image.Point, 0, len(values))

	labelBarColor := barColor
	lineColor := darken(barColor, 0.3)
	fillColor := lighten(barColor, 0.35)

	for i, v := range values {
		cx := plotX0 + int((float64(i)+0.5)*slotW)
		x0 := cx - barW/2
		x1 := x0 + barW
		h := int(math.Round(float64(v) / float64(maxBar) * float64(plotH)))
		if h < 1 && v > 0 {
			h = 1
		}
		y0 := plotY1 - h
		fillRect(img, x0, y0, x1, plotY1, fillColor)
		drawLine(img, x0, y0, x0, plotY1, labelBarColor)
		drawLine(img, x1, y0, x1, plotY1, labelBarColor)
		drawLine(img, x0, y0, x1, y0, labelBarColor)

		if len(values) <= 120 && v > 0 {
			label := formatInt(v)
			tw := measureString(label, 1)
			drawString(img, cx-tw/2, y0-10, label, 1, Black)
		}

		ch := int(math.Round(float64(cumValues[i]) / float64(maxCum) * float64(plotH)))
		if ch < 1 && cumValues[i] > 0 {
			ch = 1
		}
		linePts = append(linePts, image.Point{X: cx, Y: plotY1 - ch})
	}

	drawPolyline(img, linePts, lineColor)
	for _, pt := range linePts {
		fillRect(img, pt.X-2, pt.Y-2, pt.X+3, pt.Y+3, lineColor)
	}

	for i := 0; i <= 5; i++ {
		lv := int64(math.Round(float64(maxBar) * float64(i) / 5.0))
		rv := int64(math.Round(float64(maxCum) * float64(i) / 5.0))
		y := plotY1 - int(float64(plotH)*float64(i)/5.0)
		left := formatInt(lv)
		right := formatInt(rv)
		drawString(img, plotX0-measureString(left, 1)-8, y-4, left, 1, Black)
		drawString(img, plotX1+8, y-4, right, 1, Black)
	}

	labels := simplifyXLabels(xLabels, maxLabels)
	for i, label := range labels {
		if strings.TrimSpace(label) == "" {
			continue
		}
		label = truncateLabel(label, 12)
		x := plotX0 + int((float64(i)+0.5)*slotW)
		drawLine(img, x, plotY1, x, plotY1+5, Black)
		tw := measureString(label, 1)
		drawString(img, x-tw/2, plotY1+10, label, 1, Black)
	}

	return savePNG(path, img)
}

func PlotTotals(title string, labels []string, values []int64, path string, colors []color.RGBA) error {
	width, height := 900, 420
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	fillRect(img, 0, 0, width, height, White)

	marginLeft, marginRight := 70, 50
	marginTop, marginBottom := 45, 70
	plotX0, plotY0 := marginLeft, marginTop
	plotX1, plotY1 := width-marginRight, height-marginBottom
	plotW, plotH := plotX1-plotX0, plotY1-plotY0

	fillRect(img, plotX0, plotY0, plotX1, plotY1, color.RGBA{248, 248, 248, 255})
	for i := 0; i <= 5; i++ {
		y := plotY1 - int(float64(plotH)*float64(i)/5.0)
		drawLine(img, plotX0, y, plotX1, y, Gray)
	}
	drawLine(img, plotX0, plotY0, plotX0, plotY1, Black)
	drawLine(img, plotX0, plotY1, plotX1, plotY1, Black)
	drawString(img, 20, 15, title, 2, Black)

	if len(values) == 0 {
		drawString(img, plotX0+20, plotY0+20, "NO DATA", 2, Black)
		return savePNG(path, img)
	}
	maxV := maxInt64(values)
	slotW := float64(plotW) / float64(len(values))
	barW := int(slotW * 0.55)
	if barW < 1 {
		barW = 1
	}
	for i, v := range values {
		cx := plotX0 + int((float64(i)+0.5)*slotW)
		x0 := cx - barW/2
		x1 := x0 + barW
		h := int(math.Round(float64(v) / float64(maxV) * float64(plotH)))
		if h < 1 && v > 0 {
			h = 1
		}
		y0 := plotY1 - h
		fill := color.RGBA{150, 180, 230, 255}
		if i < len(colors) {
			fill = lighten(colors[i], 0.25)
		}
		stroke := darken(fill, 0.35)
		fillRect(img, x0, y0, x1, plotY1, fill)
		drawLine(img, x0, y0, x1, y0, stroke)
		drawLine(img, x0, y0, x0, plotY1, stroke)
		drawLine(img, x1, y0, x1, plotY1, stroke)
		label := formatInt(v)
		drawString(img, cx-measureString(label, 1)/2, y0-10, label, 1, Black)
		if i < len(labels) {
			l := truncateLabel(labels[i], 12)
			drawString(img, cx-measureString(l, 1)/2, plotY1+10, l, 1, Black)
		}
	}
	for i := 0; i <= 5; i++ {
		lv := int64(math.Round(float64(maxV) * float64(i) / 5.0))
		y := plotY1 - int(float64(plotH)*float64(i)/5.0)
		left := formatInt(lv)
		drawString(img, plotX0-measureString(left, 1)-8, y-4, left, 1, Black)
	}
	return savePNG(path, img)
}

var font5x7 = map[rune][]string{
	' ': {".....", ".....", ".....", ".....", ".....", ".....", "....."},
	'-': {".....", ".....", ".....", ".###.", ".....", ".....", "....."},
	'.': {".....", ".....", ".....", ".....", ".....", ".##..", ".##.."},
	',': {".....", ".....", ".....", ".....", ".##..", ".##..", "##..."},
	':': {".....", ".##..", ".##..", ".....", ".##..", ".##..", "....."},
	'+': {".....", "..#..", "..#..", ".###.", "..#..", "..#..", "....."},
	'/': {"....#", "...#.", "...#.", "..#..", ".#...", ".#...", "#...."},
	'(': {"...#.", "..#..", ".#...", ".#...", ".#...", "..#..", "...#."},
	')': {".#...", "..#..", "...#.", "...#.", "...#.", "..#..", ".#..."},
	'_': {".....", ".....", ".....", ".....", ".....", ".....", "#####"},
	'0': {".###.", "#...#", "#..##", "#.#.#", "##..#", "#...#", ".###."},
	'1': {"..#..", ".##..", "..#..", "..#..", "..#..", "..#..", ".###."},
	'2': {".###.", "#...#", "....#", "...#.", "..#..", ".#...", "#####"},
	'3': {"#####", "....#", "...#.", "..##.", "....#", "#...#", ".###."},
	'4': {"...#.", "..##.", ".#.#.", "#..#.", "#####", "...#.", "...#."},
	'5': {"#####", "#....", "####.", "....#", "....#", "#...#", ".###."},
	'6': {".###.", "#...#", "#....", "####.", "#...#", "#...#", ".###."},
	'7': {"#####", "....#", "...#.", "..#..", ".#...", ".#...", ".#..."},
	'8': {".###.", "#...#", "#...#", ".###.", "#...#", "#...#", ".###."},
	'9': {".###.", "#...#", "#...#", ".####", "....#", "#...#", ".###."},
	'A': {".###.", "#...#", "#...#", "#####", "#...#", "#...#", "#...#"},
	'B': {"####.", "#...#", "#...#", "####.", "#...#", "#...#", "####."},
	'C': {".###.", "#...#", "#....", "#....", "#....", "#...#", ".###."},
	'D': {"####.", "#...#", "#...#", "#...#", "#...#", "#...#", "####."},
	'E': {"#####", "#....", "#....", "####.", "#....", "#....", "#####"},
	'F': {"#####", "#....", "#....", "####.", "#....", "#....", "#...."},
	'G': {".###.", "#...#", "#....", "#.###", "#...#", "#...#", ".###."},
	'H': {"#...#", "#...#", "#...#", "#####", "#...#", "#...#", "#...#"},
	'I': {".###.", "..#..", "..#..", "..#..", "..#..", "..#..", ".###."},
	'J': {"..###", "...#.", "...#.", "...#.", "...#.", "#..#.", ".##.."},
	'K': {"#...#", "#..#.", "#.#..", "##...", "#.#..", "#..#.", "#...#"},
	'L': {"#....", "#....", "#....", "#....", "#....", "#....", "#####"},
	'M': {"#...#", "##.##", "#.#.#", "#.#.#", "#...#", "#...#", "#...#"},
	'N': {"#...#", "##..#", "##..#", "#.#.#", "#..##", "#..##", "#...#"},
	'O': {".###.", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."},
	'P': {"####.", "#...#", "#...#", "####.", "#....", "#....", "#...."},
	'Q': {".###.", "#...#", "#...#", "#...#", "#.#.#", "#..#.", ".##.#"},
	'R': {"####.", "#...#", "#...#", "####.", "#.#..", "#..#.", "#...#"},
	'S': {".####", "#....", "#....", ".###.", "....#", "....#", "####."},
	'T': {"#####", "..#..", "..#..", "..#..", "..#..", "..#..", "..#.."},
	'U': {"#...#", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."},
	'V': {"#...#", "#...#", "#...#", "#...#", ".#.#.", ".#.#.", "..#.."},
	'W': {"#...#", "#...#", "#...#", "#.#.#", "#.#.#", "##.##", "#...#"},
	'X': {"#...#", ".#.#.", ".#.#.", "..#..", ".#.#.", ".#.#.", "#...#"},
	'Y': {"#...#", ".#.#.", ".#.#.", "..#..", "..#..", "..#..", "..#.."},
	'Z': {"#####", "....#", "...#.", "..#..", ".#...", "#....", "#####"},
}

func measureString(s string, scale int) int {
	if scale < 1 {
		scale = 1
	}
	width := 0
	for _, r := range strings.ToUpper(s) {
		glyph, ok := font5x7[r]
		if !ok {
			glyph = font5x7[' ']
		}
		if len(glyph) > 0 {
			width += (len(glyph[0]) + 1) * scale
		}
	}
	if width > 0 {
		width -= scale
	}
	return width
}

func drawString(img *image.RGBA, x, y int, s string, scale int, c color.Color) {
	if scale < 1 {
		scale = 1
	}
	ox := x
	for _, r := range strings.ToUpper(s) {
		glyph, ok := font5x7[r]
		if !ok {
			glyph = font5x7[' ']
		}
		for gy, row := range glyph {
			for gx, ch := range row {
				if ch == '#' {
					fillRect(img, ox+gx*scale, y+gy*scale, ox+(gx+1)*scale, y+(gy+1)*scale, c)
				}
			}
		}
		ox += (len(glyph[0]) + 1) * scale
	}
}

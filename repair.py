from pathlib import Path
import subprocess

path = Path('k8s_sidecar.go')
expected = '95f3619f1b9d11b7cfbc78437d24c18d6278e29b'
actual = subprocess.check_output(['git', 'hash-object', str(path)], text=True).strip()
if actual != expected:
    raise SystemExit(f'source changed: expected {expected}, got {actual}')
text = path.read_text()
helper = '''// sidecarLabelSetEnd ignores braces inside quoted label values. A backslash
// escapes the next byte only within a quoted value.
func sidecarLabelSetEnd(line string, open int) int {
\tquoted := false
\tfor i := open + 1; i < len(line); i++ {
\t\tswitch line[i] {
\t\tcase '\\\\':
\t\t\tif quoted {
\t\t\t\ti++
\t\t\t}
\t\tcase '\"':
\t\t\tquoted = !quoted
\t\tcase '}':
\t\t\tif !quoted {
\t\t\t\treturn i
\t\t\t}
\t\t}
\t}
\treturn -1
}

'''
replacements = [
    ('spaceIdx := strings.Index(line, " ")', 'spaceIdx := strings.IndexAny(line, " \\t")'),
    ('closeBraceIdx := strings.Index(line, "}")', 'closeBraceIdx := sidecarLabelSetEnd(line, braceIdx)'),
    (r'''var sidecarLabelRegex = regexp.MustCompile(`(\w+)="([^"]*)"`)''',
     helper + r'''var sidecarLabelRegex = regexp.MustCompile(`(\w+)="([^"\\]*(?:\\.[^"\\]*)*)"`)

// Replacer consumes the input once, so an escaped backslash followed by n does
// not become a newline on a second decoding pass.
var sidecarLabelUnescaper = strings.NewReplacer(`\\`, `\`, `\"`, `"`, `\n`, "\n")'''),
    ('labels[match[1]] = match[2]', '''value := match[2]
\t\t\tif strings.Contains(value, "\\\\") {
\t\t\t\tvalue = sidecarLabelUnescaper.Replace(value)
\t\t\t}
\t\t\tlabels[match[1]] = value'''),
]
for before, after in replacements:
    if text.count(before) != 1:
        raise SystemExit(f'expected exactly one source match: {before!r}')
    text = text.replace(before, after)
path.write_text(text)

from pathlib import Path
import subprocess

path = Path('k8s_sidecar.go')
expected = '55ee94e0c3e3c54f672505afa95890292029a3bd'
actual = subprocess.check_output(['git', 'hash-object', str(path)], text=True).strip()
if actual != expected:
    raise SystemExit(f'source changed: expected {expected}, got {actual}')
text = path.read_text()
start = text.index('func (s *K8sSidecar) parseMetricLine(')
end = text.index('\n// sidecarLabelSetEnd', start)
function = text[start:end]
old = '''\t\t\t_, err := fmt.Sscanf(parts[0], "%f", &point.Value)
\t\t\tif err != nil {
\t\t\t\treturn point, err
\t\t\t}'''
new = '''\t\t\tvalue, err := strconv.ParseFloat(parts[0], 64)
\t\t\tif err != nil {
\t\t\t\treturn point, err
\t\t\t}
\t\t\tpoint.Value = value'''
if function.count(old) != 2:
    raise SystemExit('expected exactly two numeric conversions')
text = text[:start] + function.replace(old, new) + text[end:]
if text.count('\t"regexp"\n') != 1 or '\t"strconv"\n' in text:
    raise SystemExit('unexpected import layout')
text = text.replace('\t"regexp"\n', '\t"regexp"\n\t"strconv"\n')
path.write_text(text)

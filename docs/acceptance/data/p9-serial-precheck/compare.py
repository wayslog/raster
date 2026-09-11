from pathlib import Path
import csv,hashlib,io,json,os,statistics,subprocess,sys
root=Path('target/p9-local-serial-experiment');out=root/sys.argv[1];out.mkdir(exist_ok=False)
common=Path('target/p9-perf-common.rs').read_bytes();assert Path('target/p9-perf-current/examples/memory_matrix.rs').read_bytes()==common
binaries={'baseline':root/'baseline-memory_matrix','candidate':Path('target/p9-perf-current/target/release/examples/memory_matrix')};results={k:[] for k in binaries};env=os.environ.copy();env['RASTER_PERF_HOT_ONLY']='1'
for trial in range(3):
 order=['baseline','candidate'] if (trial+(sys.argv[1]=='round-2'))%2==0 else ['candidate','baseline']
 for name in order:
  p=subprocess.run([str(binaries[name])],env=env,text=True,capture_output=True,timeout=60);(out/f'{trial}-{name}.csv').write_text(p.stdout);(out/f'{trial}-{name}.log').write_text(p.stderr.replace(str(Path.cwd()),'工作树'));assert p.returncode==0
  rows=list(csv.DictReader(io.StringIO(p.stdout)));assert len(rows)==3;assert all(r['布局']=='原子u64' and r['分布']=='单键热点' and r['线程']=='1' and r['操作数']=='200000' and r['拒绝重试']=='0' for r in rows)
  results[name]+=rows
medians={name:{k:statistics.median(float(r[k]) for r in rows) for k in ['每秒操作','P50纳秒','P95纳秒','P99纳秒']} for name,rows in results.items()}
report={'基线提交':'be5ac0e93a0462be46d3b424918687ab50fe5a21','模式':'all features release','每端真实样本':9,'驱动SHA256':hashlib.sha256(common).hexdigest(),'二进制SHA256':{k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in binaries.items()},'候选源码SHA256':{name:hashlib.sha256((Path('target/p9-perf-current')/name).read_bytes()).hexdigest() for name in ['src/api/completion.rs','src/engine/upsert.rs','src/engine/mod.rs']},'中位数':medians,'吞吐比':medians['candidate']['每秒操作']/medians['baseline']['每秒操作'],'P99比':medians['candidate']['P99纳秒']/medians['baseline']['P99纳秒']}
(out/'comparison.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print(json.dumps(report,ensure_ascii=False,indent=2))

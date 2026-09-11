from pathlib import Path
import csv,hashlib,io,itertools,json,os,statistics,subprocess
root=Path('target/p9-result-pool-experiment'); out=root/'full'; out.mkdir(exist_ok=False)
binaries={'baseline':root/'baseline-memory_matrix','candidate':Path('target/p9-perf-current/target/release/examples/memory_matrix')}
env=os.environ.copy();env.pop('RASTER_PERF_HOT_ONLY',None)
axes=set(itertools.product(['原子u64','变长32至512字节'],['256键均匀','单键热点'],['1','4'],['1','2','3']))
data={k:[] for k in binaries};orders=[['baseline','candidate'],['candidate','baseline'],['candidate','baseline'],['baseline','candidate']]
(root/'binaries.json').write_text(json.dumps({name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in binaries.items()},indent=2))
for trial,order in enumerate(orders):
 for name in order:
  run=subprocess.run([str(binaries[name])],env=env,text=True,capture_output=True,timeout=180)
  (out/f'{trial}-{name}.csv').write_text(run.stdout)
  (out/f'{trial}-{name}.log').write_text(run.stderr.replace(str(Path.cwd()),'工作树'))
  assert run.returncode==0,(trial,name,run.returncode)
  rows=list(csv.DictReader(io.StringIO(run.stdout)))
  assert len(rows)==24 and {tuple(r[k] for k in ['布局','分布','线程','轮次']) for r in rows}==axes
  assert all(r['操作数']=='200000' for r in rows)
  data[name]+=rows
  print(f'样本 {trial} {name} 完成，24 个场景',flush=True)
report=[]
for layout,distribution,threads in itertools.product(['原子u64','变长32至512字节'],['256键均匀','单键热点'],['1','4']):
 medians={}
 for name,rows in data.items():
  samples=[r for r in rows if (r['布局'],r['分布'],r['线程'])==(layout,distribution,threads)]
  assert len(samples)==12
  medians[name]={metric:statistics.median(float(row[metric]) for row in samples) for metric in ['每秒操作','P99纳秒']}
 report.append({'布局':layout,'分布':distribution,'线程':threads,'每端样本':12,'中位数':medians,'吞吐比':medians['candidate']['每秒操作']/medians['baseline']['每秒操作'],'P99比':medians['candidate']['P99纳秒']/medians['baseline']['P99纳秒']})
(out/'comparison.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps(report,ensure_ascii=False,indent=2))

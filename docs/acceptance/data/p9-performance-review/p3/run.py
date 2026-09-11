"""保持原输入、计时与复核线，比较完整实现和旧 P3 的八组工作量。"""
from pathlib import Path
import csv,hashlib,io,itertools,json,os,statistics,subprocess
root=Path('target/p9-final-p3-review');out=root/'full';out.mkdir(exist_ok=False)
plan=json.loads((root/'plan.json').read_text())
binaries={name:Path('target/p9-perf-'+name+'/target/release/examples/memory_matrix') for name in ['old','current']}
(root/'binaries.json').write_text(json.dumps({k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in binaries.items()},indent=2)+'\n')
env=os.environ.copy();env.pop('RASTER_PERF_HOT_ONLY',None)
axes=set(itertools.product(['原子u64','变长32至512字节'],['256键均匀','单键热点'],['1','4'],['1','2','3']))
data={name:[] for name in binaries}
for repeat,order in enumerate(plan['顺序']):
 for name in order:
  process=subprocess.run([str(binaries[name])],env=env,text=True,capture_output=True,timeout=240)
  (out/f'{repeat}-{name}.csv').write_text(process.stdout)
  (out/f'{repeat}-{name}.txt').write_text(process.stderr.replace(str(Path.cwd()),'工作树'))
  assert process.returncode==0,(repeat,name,process.returncode)
  rows=list(csv.DictReader(io.StringIO(process.stdout)))
  assert len(rows)==24 and {tuple(r[k] for k in ['布局','分布','线程','轮次']) for r in rows}==axes
  assert all(r['操作数']=='200000' for r in rows)
  data[name]+=rows
  print(repeat,name,'24 个样本完成',flush=True)
reports=[]
for layout,distribution,threads in itertools.product(['原子u64','变长32至512字节'],['256键均匀','单键热点'],['1','4']):
 medians={}
 for name,rows in data.items():
  selected=[r for r in rows if (r['布局'],r['分布'],r['线程'])==(layout,distribution,threads)]
  assert len(selected)==12
  medians[name]={key:statistics.median(float(row[key]) for row in selected) for key in ['每秒操作','P50纳秒','P95纳秒','P99纳秒','拒绝重试']}
 speed=medians['current']['每秒操作']/medians['old']['每秒操作'];latency=medians['current']['P99纳秒']/medians['old']['P99纳秒']
 reports.append({'布局':layout,'分布':distribution,'线程':threads,'每端样本':12,'中位数':medians,'吞吐比':speed,'P99比':latency,'触发复核':speed<.75 or latency>1.3})
(out/'comparison.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(reports,ensure_ascii=False,indent=2))
raise SystemExit(int(any(r['触发复核'] for r in reports)))

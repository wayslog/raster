from pathlib import Path
import csv,io,json,os,statistics,subprocess,hashlib
root=Path('target/p9-final-cost-review');out=root/'old-pair';out.mkdir(exist_ok=False)
env={**os.environ,'RASTER_COST_DIAGNOSTIC':'仅单线程驻留内存','RASTER_COST_OPS':'20000000'};names=['m0v0o0','old-p3'];data={n:[] for n in names}
for trial in range(3):
 for name in (names if trial%2==0 else names[::-1]):
  p=subprocess.run([str(root/name)],env=env,text=True,capture_output=True,timeout=90);assert p.returncode==0,p.stderr
  (out/f'{trial}-{name}.csv').write_text(p.stdout);r=list(csv.DictReader(io.StringIO(p.stdout)));assert len(r)==1 and r[0]['读取结果']=='20001000' and r[0]['接受序号']=='20001001';data[name]+=r
 medians={name:statistics.median(float(r['耗时纳秒'])/int(r['操作数']) for r in rows) for name,rows in data.items()}
 report={'每端样本':trial+1,'每操作纳秒中位数':medians,'当前与旧P3成本比':medians[names[0]]/medians[names[1]],'二进制SHA256':{n:hashlib.sha256((root/n).read_bytes()).hexdigest() for n in names}}
 (out/'comparison.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print(trial,'完成',flush=True)
print(json.dumps(report,ensure_ascii=False,indent=2))

from pathlib import Path
import csv,hashlib,io,json,os,statistics,subprocess
root=Path('target/p9-result-pool-experiment'); out=root/'long';out.mkdir(exist_ok=False)
env=os.environ.copy();env.pop('RASTER_PERF_HOT_ONLY',None);env['RASTER_PERF_BYTES_HOT']='1'
orders=[['baseline','candidate'],['candidate','baseline'],['candidate','baseline'],['baseline','candidate']]
reports={}
for mode in ['校准AA','复核AB']:
 data={'baseline':[],'candidate':[]}
 for trial,order in enumerate(orders):
  for name in order:
   binary=root/('long-'+('baseline' if mode=='校准AA' else name))
   run=subprocess.run([str(binary)],env=env,text=True,capture_output=True,timeout=60)
   (out/f'{mode}-{trial}-{name}.csv').write_text(run.stdout)
   (out/f'{mode}-{trial}-{name}.log').write_text(run.stderr.replace(str(Path.cwd()),'工作树'))
   assert run.returncode==0
   rows=list(csv.DictReader(io.StringIO(run.stdout)));assert len(rows)==3
   assert all((r['布局'],r['分布'],r['线程'],r['操作数'])==('变长32至512字节','单键热点','4','2000000') for r in rows)
   data[name]+=rows
   print(mode,trial,name,'通过',flush=True)
 medians={name:{metric:statistics.median(float(r[metric]) for r in rows) for metric in ['每秒操作','P99纳秒','拒绝重试']} for name,rows in data.items()}
 reports[mode]={'每端样本':12,'中位数':medians,'吞吐比':medians['candidate']['每秒操作']/medians['baseline']['每秒操作'],'P99比':medians['candidate']['P99纳秒']/medians['baseline']['P99纳秒']}
(out/'comparison.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2));print(json.dumps(reports,ensure_ascii=False,indent=2))

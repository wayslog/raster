from pathlib import Path
import csv,io,json,os,statistics,subprocess
root=Path('target/p9-final-cost-review');out=root/'paired';out.mkdir(exist_ok=False)
env={**os.environ,'RASTER_COST_DIAGNOSTIC':'仅单线程驻留内存','RASTER_COST_OPS':'20000000'}
manifest=json.loads((root/'manifest.json').read_text());base='m0v0o0'
def run(binary,stem,count):
 e={**env,'RASTER_COST_OPS':str(count)};p=subprocess.run([str(root/binary)],env=e,capture_output=True,text=True,timeout=90)
 (out/(stem+'.csv')).write_text(p.stdout);(out/(stem+'.txt')).write_text(p.stderr.replace(str(Path.cwd()),'工作树'))
 assert p.returncode==0,(stem,p.stderr)
 rows=list(csv.DictReader(io.StringIO(p.stdout)));assert len(rows)==1
 row=rows[0];assert row['操作数']==str(count) and row['预热']=='1000' and int(row['读取结果'])==count+1000 and int(row['接受序号'])==count+1001
 return row
for v in manifest['变体']:run(v['名称'],'smoke-'+v['名称'],1000000)
print('八个受限变体的实际值、接受序号和关闭冒烟检查通过',flush=True)
reports=[]
for v in manifest['变体']:
 name=v['名称']
 if name==base:continue
 data={base:[],name:[]}
 for repeat in range(3):
  order=[base,name] if repeat%2==0 else [name,base]
  for binary in order:data[binary].append(run(binary,f'{name}-{repeat}-{binary}',20000000))
  print(name,'成对样本',repeat,'完成',flush=True)
 medians={binary:statistics.median(float(r['每秒操作']) for r in rows) for binary,rows in data.items()}
 ns={binary:statistics.median(float(r['耗时纳秒'])/int(r['操作数']) for r in rows) for binary,rows in data.items()}
 reports.append({**v,'每端样本':3,'吞吐中位数':medians,'每操作纳秒中位数':ns,'吞吐比':medians[name]/medians[base],'每操作节省纳秒':ns[base]-ns[name]})
 (out/'comparison.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2))
print(json.dumps(reports,ensure_ascii=False,indent=2))

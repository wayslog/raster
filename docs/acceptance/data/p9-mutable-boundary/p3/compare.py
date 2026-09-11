import csv,hashlib,io,json,os,statistics,subprocess
from pathlib import Path
root=Path('target');out=root/'p9-perf-pair-011';out.mkdir(exist_ok=False)
env=os.environ.copy();env['RASTER_PERF_HOT_ONLY']='1';results={};meta={'驱动SHA256':hashlib.sha256((root/'p9-perf-common.rs').read_bytes()).hexdigest(),'工具链':subprocess.check_output(['rustc','-Vv'],text=True),'特性':'all','模式':'release','场景':'原子u64 单键热点 单线程 每轮200000 三轮','执行顺序':['current','old'],'结果':{}}
for name in ['current','old']:
 wt=root/('p9-perf-'+name);binary=wt/'target/release/examples/memory_matrix'
 p=subprocess.run([str(binary)],env=env,capture_output=True,text=True,timeout=60)
 (out/(name+'.csv')).write_text(p.stdout);(out/(name+'.log')).write_text(p.stderr.replace(str(Path.cwd()),'工作树'))
 assert p.returncode==0,p.stderr
 rows=list(csv.DictReader(io.StringIO(p.stdout)));assert len(rows)==3
 assert all(r['操作数']=='200000' and r['线程']=='1' and r['布局']=='原子u64' and r['分布']=='单键热点' and r['拒绝重试']=='0' for r in rows)
 m={k:statistics.median(float(r[k]) for r in rows) for k in ['每秒操作','P50纳秒','P95纳秒','P99纳秒']}
 results[name]=m
 meta['结果'][name]={'提交':subprocess.check_output(['git','-C',str(wt),'rev-parse','HEAD'],text=True).strip(),'二进制SHA256':hashlib.sha256(binary.read_bytes()).hexdigest(),'中位数':m}
meta['结果']['current']['候选补丁SHA256']=hashlib.sha256(Path('target/p9-mutable-boundary-experiment/candidate.patch').read_bytes()).hexdigest()
meta['结果']['current']['来源']='基线提交加候选补丁；生产算法与正式变更相同'
ratio=results['current']['每秒操作']/results['old']['每秒操作'];p99=results['current']['P99纳秒']/results['old']['P99纳秒'];code=int(ratio<.75 or p99>1.3)
meta.update(吞吐比=ratio,P99比=p99,退出码=code)
(out/'comparison.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2));print(json.dumps(meta,ensure_ascii=False,indent=2));raise SystemExit(code)

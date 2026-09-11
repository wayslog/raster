from pathlib import Path
import csv,io,json,os,subprocess
root=Path('target/p9-cost-attribution');env={**os.environ,'RASTER_COST_DIAGNOSTIC':'仅单线程驻留内存','RASTER_COST_OPS':'40000000'}
results=[]
for name,duration in [('profile-baseline',3),('old-p3',1)]:
 p=subprocess.Popen([str(root/name)],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 marker=p.stderr.readline();assert marker.strip()=='诊断窗口就绪'
 sample=subprocess.run(['sample',str(p.pid),str(duration),'-file',str(root/(name+'-sample.txt'))],text=True,capture_output=True,timeout=30)
 out,err=p.communicate(timeout=90);assert p.returncode==0
 rows=list(csv.DictReader(io.StringIO(out)));assert len(rows)==1 and rows[0]['读取结果']=='40001000' and rows[0]['接受序号']=='40001001'
 (root/(name+'-profile-run.csv')).write_text(out);(root/(name+'-profile-command.txt')).write_text((sample.stdout+sample.stderr+marker+err).replace(str(Path.cwd()),'工作树').replace(str(Path.home()),'用户目录').rstrip()+'\n')
 sample_path=root/(name+'-sample.txt');assert sample.returncode==0 and sample_path.exists()
 sample_path.write_text(sample_path.read_text().replace(str(Path.cwd()),'工作树').replace(str(Path.home()),'用户目录'))
 results.append({'程序':name,'请求采样秒数':duration,'程序退出码':p.returncode,'采样退出码':sample.returncode,'实际业务操作数':40000000});print(name,'采样和实际值校验完成',flush=True)
(root/'profile-results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))

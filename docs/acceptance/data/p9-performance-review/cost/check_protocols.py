"""证明诊断移除项破坏真实协议；失败变体只存在于隔离检出。"""
from pathlib import Path
import json,re,subprocess
root=Path.cwd();out=root/'target/p9-final-cost-review/protocols';out.mkdir(exist_ok=False)
wt=root/'target/p9-cost-profile';source_root=out.parent
sources={name:(source_root/(name+'.baseline.rs')).read_bytes() for name in ['io_hub','version_permit','upsert']}
cases=[
 ('m1v0o0','engine::io_hub::tests::其他会话轮询后结果留在原邮箱且错身份不能收取'),
 ('m0v1o0','engine::version_permit::tests::旧版本全部退出后新版本才可执行且不同键互不阻塞'),
 ('m0v0o1','engine::pending_read_tests::提交和轮询自动观察版本且旧请求保持原切分'),
]
def clean(text):return text.replace(str(root.parent)+'/','工作区/').replace(str(Path.home()),'用户目录')
report=[]
try:
 for name,test in cases:
  command=['cargo','test','--locked','--all-features','--lib',test,'--','--exact','--nocapture']
  normal=subprocess.run(command,cwd=root,capture_output=True,text=True,timeout=180)
  text=clean(normal.stdout+normal.stderr);(out/(name+'-normal.txt')).write_text(text)
  assert normal.returncode==0 and 'test result: ok. 1 passed; 0 failed;' in text
  subprocess.run(['git','apply','--unidiff-zero',str(source_root/(name+'.patch'))],cwd=wt,check=True)
  control=subprocess.run(command,cwd=wt,capture_output=True,text=True,timeout=180)
  text=clean(control.stdout+control.stderr);(out/(name+'-control.txt')).write_text(text)
  assert control.returncode==101 and 'test result: FAILED. 0 passed; 1 failed;' in text,(name,control.returncode)
  report.append({'变体':name,'真实测试':test,'正常退出码':normal.returncode,'移除后退出码':control.returncode})
  (out/'comparison.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
  print(name,'正常通过、移除后明确失败',flush=True)
  for module,data in sources.items():(wt/f'src/engine/{module}.rs').write_bytes(data)
finally:
 for module,data in sources.items():(wt/f'src/engine/{module}.rs').write_bytes(data)
 assert not subprocess.check_output(['git','diff','--name-only','--','src'],cwd=wt)

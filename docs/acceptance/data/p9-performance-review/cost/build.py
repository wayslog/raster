from pathlib import Path
import hashlib,itertools,json,shutil,subprocess
root=Path('target/p9-final-cost-review');wt=Path('target/p9-cost-profile');sources={n:(root/(n+'.baseline.rs')).read_text() for n in ['io_hub','version_permit','upsert']}
def body(source, marker, replacement):
 start=source.index(marker);left=source.index('{',start);depth=1;right=left+1
 while depth:
  depth += (source[right]=='{')-(source[right]=='}');right+=1
 return source[:left+1]+'\n'+replacement+'\n'+source[right-1:]
manifest={'版本':1,'基线提交':subprocess.check_output(['git','-C',str(wt),'rev-parse','HEAD'],text=True).strip(),'用途':'仅单线程驻留内存成本归因；移除协议的变体不是可交付引擎','变体':[]}
for mailbox,version,observation in itertools.product([False,True],repeat=3):
 name=f'm{int(mailbox)}v{int(version)}o{int(observation)}';code=sources.copy()
 if mailbox:
  code['io_hub']=body(code['io_hub'],'pub fn reserve(&self, session:', 'session.validate()?; Ok(RequestId { store:self.store, session, slot:0, generation:Generation(0) })')
  code['io_hub']=body(code['io_hub'],'pub fn release(&self, id:', 'let _ = id; Ok(())')
 if version:
  code['version_permit']=body(code['version_permit'],'pub fn reserve(', 'Ok(VersionPermit { active:self.active.clone(), key:(hash.0,version.0) })')
  code['version_permit']=body(code['version_permit'],'pub fn ready(&self)', 'Ok(true)')
  code['version_permit']=body(code['version_permit'],'fn drop(&mut self)', '')
 if observation:
  segment='        if let Err(reason) = self.observe_session(session) {\n            return Err(Rejected { request, reason });\n        }\n'
  assert code['upsert'].count(segment)==1
  code['upsert']=code['upsert'].replace(segment,'')
 for n,s in code.items():(wt/f'src/engine/{n}.rs').write_text(s)
 with (root/(name+'-build.txt')).open('w') as stream:
  p=subprocess.run(['cargo','build','--locked','--release','--all-features','--example','cost_attribution'],cwd=wt,text=True,stdout=stream,stderr=subprocess.STDOUT)
 text=(root/(name+'-build.txt')).read_text().replace(str(Path.cwd()),'工作树').replace(str(Path.home()),'用户目录');(root/(name+'-build.txt')).write_text(text)
 assert p.returncode==0,name
 binary=wt/'target/release/examples/cost_attribution';shutil.copy2(binary,root/name)
 (root/(name+'.patch')).write_bytes(subprocess.check_output(['git','-C',str(wt),'diff','--unified=0','--','src/engine']))
 manifest['变体'].append({'名称':name,'移除邮箱登记与注销':mailbox,'移除版本表登记与等待':version,'移除写入入口阶段观察':observation,'二进制SHA256':hashlib.sha256(binary.read_bytes()).hexdigest()})
 (root/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2));print(name,'构建完成',flush=True)
for n,s in sources.items():(wt/f'src/engine/{n}.rs').write_text(s)

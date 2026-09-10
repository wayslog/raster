//! 测试专用掉电模型：文件内容与目录项分别持久化，掉电丢弃未同步状态。
use crate::device::{FileId, IoCompletion, IoOperation, IoOutcome};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(super) enum Change {
    Open(PathBuf),
    Directory(PathBuf),
    SyncFile(FileId),
    SyncDirectory(PathBuf),
    Rename(PathBuf, PathBuf),
    Other,
}
impl Change {
    pub(super) fn from_operation(operation: &IoOperation) -> Self {
        match operation {
            IoOperation::Open { path, .. } => Self::Open(path.clone()),
            IoOperation::CreateDirectory(path) => Self::Directory(path.clone()),
            IoOperation::SyncFile { file, .. } => Self::SyncFile(*file),
            IoOperation::SyncDirectory(path) => Self::SyncDirectory(path.clone()),
            IoOperation::Rename {
                source,
                destination,
            } => Self::Rename(source.clone(), destination.clone()),
            _ => Self::Other,
        }
    }
}
#[derive(Default)]
pub(super) struct DurableModel {
    paths: BTreeMap<PathBuf, usize>,
    handles: BTreeMap<u64, PathBuf>,
    nodes: Vec<Node>,
}
#[derive(Default)]
struct Node {
    directory: bool,
    data: Vec<u8>,
    children: BTreeMap<String, usize>,
}
impl DurableModel {
    fn ensure(&mut self, path: &Path, directory: bool) -> usize {
        if let Some(&id) = self.paths.get(path) {
            return id;
        }
        let id = self.nodes.len();
        self.nodes.push(Node {
            directory,
            ..Default::default()
        });
        self.paths.insert(path.to_path_buf(), id);
        id
    }
    pub(super) fn apply(&mut self, root: &Path, change: Change, completion: &IoCompletion) {
        self.ensure(Path::new(""), true);
        assert!(completion.result.is_ok());
        match change {
            Change::Open(path) => {
                self.ensure(&path, false);
                let Ok(IoOutcome::Opened(file)) = &completion.result else {
                    panic!("打开完成类型错误")
                };
                self.handles.insert(file.slot, path);
            }
            Change::Directory(path) => {
                self.ensure(&path, true);
            }
            Change::SyncFile(file) => {
                let path = &self.handles[&file.slot];
                let id = self.paths[path];
                // 完成真实同步后保存内容；后续未同步写入不会修改此快照。
                self.nodes[id].data = std::fs::read(root.join(path)).unwrap();
            }
            Change::SyncDirectory(path) => {
                let id = self.paths[&path];
                self.nodes[id].children = self
                    .paths
                    .iter()
                    .filter(|(p, _)| {
                        !p.as_os_str().is_empty() && p.parent() == Some(path.as_path())
                    })
                    .map(|(p, &id)| (p.file_name().unwrap().to_str().unwrap().to_owned(), id))
                    .collect();
            }
            Change::Rename(source, destination) => {
                let id = self.paths.remove(&source).expect("重命名源已登记");
                self.paths.insert(destination, id);
            }
            Change::Other => {}
        }
    }
    pub(super) fn materialize(&self, root: &Path) {
        assert!(!root.exists(), "掉电镜像必须使用新目录");
        let id = self.paths[Path::new("")];
        self.write_node(id, root);
    }
    fn write_node(&self, id: usize, path: &Path) {
        let node = &self.nodes[id];
        if node.directory {
            std::fs::create_dir_all(path).unwrap();
            for (name, &child) in &node.children {
                self.write_node(child, &path.join(name));
            }
        } else {
            std::fs::write(path, &node.data).unwrap();
        }
    }
}

#[test]
fn 掉电模型分别保留文件同步与目录同步且丢弃未同步内容() {
    let base = std::env::temp_dir().join(format!(
        "raster-power-{:x?}",
        crate::types::StoreId::generate().unwrap().0
    ));
    std::fs::create_dir(&base).unwrap();
    let live = base.join("源");
    std::fs::create_dir(&live).unwrap();
    let file = FileId {
        slot: 0,
        generation: crate::types::Generation(0),
    };
    let mut model = DurableModel::default();
    let opened = IoCompletion {
        id: crate::types::IoId(1),
        route: crate::device::CompletionRoute(0),
        result: Ok(IoOutcome::Opened(file)),
        buffer: None,
    };
    let done = IoCompletion {
        id: crate::types::IoId(2),
        route: crate::device::CompletionRoute(0),
        result: Ok(IoOutcome::Done),
        buffer: None,
    };
    std::fs::write(live.join("旧名"), "已同步".as_bytes()).unwrap();
    model.apply(&live, Change::Open("旧名".into()), &opened);
    model.apply(&live, Change::SyncFile(file), &done);
    model.materialize(&base.join("无目录同步"));
    assert!(!base.join("无目录同步/旧名").exists());
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    std::fs::write(live.join("旧名"), "未同步的新值".as_bytes()).unwrap();
    model.materialize(&base.join("内容未同步"));
    assert_eq!(
        std::fs::read(base.join("内容未同步/旧名")).unwrap(),
        "已同步".as_bytes()
    );
    std::fs::rename(live.join("旧名"), live.join("新名")).unwrap();
    model.apply(&live, Change::Rename("旧名".into(), "新名".into()), &done);
    model.materialize(&base.join("重命名未同步"));
    assert!(base.join("重命名未同步/旧名").exists());
    assert!(!base.join("重命名未同步/新名").exists());
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    model.materialize(&base.join("重命名已同步"));
    assert!(!base.join("重命名已同步/旧名").exists());
    assert_eq!(
        std::fs::read(base.join("重命名已同步/新名")).unwrap(),
        "已同步".as_bytes()
    );
    std::fs::write(live.join("未同步文件"), "内容".as_bytes()).unwrap();
    model.apply(&live, Change::Open("未同步文件".into()), &opened);
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    model.materialize(&base.join("仅目录同步"));
    assert!(
        std::fs::read(base.join("仅目录同步/未同步文件"))
            .unwrap()
            .is_empty()
    );
    std::fs::remove_dir_all(base).unwrap();
}

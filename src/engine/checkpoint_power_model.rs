//! Test dedicated power-down model:File contents and directory entries are persisted separately,Discard unsynchronized status after power failure.
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
    Remove(PathBuf),
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
            IoOperation::RemoveFile(path) => Self::Remove(path.clone()),
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
                    panic!("Open completion type error")
                };
                self.handles.insert(file.slot, path);
            }
            Change::Directory(path) => {
                self.ensure(&path, true);
            }
            Change::SyncFile(file) => {
                let path = &self.handles[&file.slot];
                let id = self.paths[path];
                // Save content after real sync is complete;Subsequent unsynchronized writes will not modify this snapshot.
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
                let id = self
                    .paths
                    .remove(&source)
                    .expect("Rename source is registered");
                self.paths.insert(destination, id);
            }
            Change::Remove(path) => {
                self.paths
                    .remove(&path)
                    .expect("Delete source is registered");
            }
            Change::Other => {}
        }
    }
    pub(super) fn materialize(&self, root: &Path) {
        assert!(!root.exists(), "Power-down images must use a new directory");
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
fn the_power_off_model_retains_file_synchronization_and_directory_synchronization_respectively_and_discards_unsynchronized_content()
 {
    let base = std::env::temp_dir().join(format!(
        "raster-power-{:x?}",
        crate::types::StoreId::generate().unwrap().0
    ));
    std::fs::create_dir(&base).unwrap();
    let live = base.join("source");
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
    std::fs::write(live.join("old_name"), "synced".as_bytes()).unwrap();
    model.apply(&live, Change::Open("old_name".into()), &opened);
    model.apply(&live, Change::SyncFile(file), &done);
    model.materialize(&base.join("No directory synchronization"));
    assert!(!base.join("No directory synchronization/old_name").exists());
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    std::fs::write(live.join("old_name"), "Unsynchronized new value".as_bytes()).unwrap();
    model.materialize(&base.join("Content not synced"));
    assert_eq!(
        std::fs::read(base.join("Content not synced/old_name")).unwrap(),
        "synced".as_bytes()
    );
    std::fs::rename(live.join("old_name"), live.join("new_name")).unwrap();
    model.apply(
        &live,
        Change::Rename("old_name".into(), "new_name".into()),
        &done,
    );
    model.materialize(&base.join("Rename not synced"));
    assert!(base.join("Rename not synced/old_name").exists());
    assert!(!base.join("Rename not synced/new_name").exists());
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    model.materialize(&base.join("Rename synchronized"));
    assert!(!base.join("Rename synchronized/old_name").exists());
    assert_eq!(
        std::fs::read(base.join("Rename synchronized/new_name")).unwrap(),
        "synced".as_bytes()
    );
    std::fs::remove_file(live.join("new_name")).unwrap();
    model.apply(&live, Change::Remove("new_name".into()), &done);
    model.materialize(&base.join("Delete unsynced"));
    assert_eq!(
        std::fs::read(base.join("Delete unsynced/new_name")).unwrap(),
        "synced".as_bytes()
    );
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    model.materialize(&base.join("Delete synchronized"));
    assert!(!base.join("Delete synchronized/new_name").exists());
    std::fs::write(live.join("Files not synced"), "content".as_bytes()).unwrap();
    model.apply(&live, Change::Open("Files not synced".into()), &opened);
    model.apply(&live, Change::SyncDirectory(PathBuf::new()), &done);
    model.materialize(&base.join("Directory sync only"));
    assert!(
        std::fs::read(base.join("Directory sync only/Files not synced"))
            .unwrap()
            .is_empty()
    );
    std::fs::remove_dir_all(base).unwrap();
}

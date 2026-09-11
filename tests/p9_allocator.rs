//! 用局部计数器验证验收分配器，不受测试运行器自身的并发分配干扰。
#[path = "../examples/resources/allocator.rs"]
mod allocation;
use allocation::{Counters, TrackingAllocator};
use std::alloc::{GlobalAlloc, Layout};

#[test]
fn 分配计数覆盖清零增长缩小及最终释放且保留原数据() {
    let counters = Counters::new();
    let allocator = TrackingAllocator::new(&counters);
    let layout = Layout::from_size_align(16, 8).unwrap();
    // SAFETY: 全部指针均由此分配器产生，检查成功后才访问；每次调整后使用新布局释放。
    unsafe {
        let pointer = allocator.alloc_zeroed(layout);
        assert!(!pointer.is_null());
        assert!(
            std::slice::from_raw_parts(pointer, 16)
                .iter()
                .all(|&byte| byte == 0)
        );
        pointer.write(42);
        assert_eq!(
            (counters.snapshot().bytes, counters.snapshot().allocations),
            (16, 1)
        );
        let pointer = allocator.realloc(pointer, layout, 64);
        assert!(!pointer.is_null());
        assert_eq!(pointer.read(), 42);
        let other_layout = Layout::from_size_align(8, 8).unwrap();
        let other = allocator.alloc(other_layout);
        assert!(!other.is_null());
        assert_eq!(
            (counters.snapshot().bytes, counters.snapshot().allocations),
            (72, 2)
        );
        allocator.dealloc(other, other_layout);
        let pointer = allocator.realloc(pointer, Layout::from_size_align(64, 8).unwrap(), 8);
        assert!(!pointer.is_null());
        assert_eq!(pointer.read(), 42);
        assert_eq!(counters.snapshot().bytes, 8);
        allocator.dealloc(pointer, other_layout);
    }
    let final_state = counters.snapshot();
    assert_eq!(
        (final_state.bytes, final_state.allocations, final_state.peak),
        (0, 0, 72)
    );
}

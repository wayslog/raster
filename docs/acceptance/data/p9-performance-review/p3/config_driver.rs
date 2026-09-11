//! 只输出两个版本共同的默认配置，供原 P3 同配置复核。
fn main() {
    let c = raster::config::Config::default();
    println!("项目,值");
    println!("页字节,{}", c.log.page_bytes);
    println!("内存页,{}", c.log.memory_pages);
    println!("可变比例,{}", c.log.mutable_fraction);
    println!("索引桶,{}", c.index.buckets);
    println!("缓存启用,{}", c.cache.enabled);
    println!("缓存容量,{}", c.cache.capacity_bytes);
    println!("自动压缩,{}", c.maintenance.auto_compaction);
    println!("维护工作者,{}", c.maintenance.workers);
    println!("会话数,{}", c.session.max_sessions);
    println!("挂起限额,{}", c.session.max_pending);
    println!("结果限额,{}", c.session.max_results);
    println!("日志预分配,{}", c.storage.pre_allocate_log);
    println!("段字节,{}", c.storage.segment_bytes);
}

"""Compatibility readers for historical localized acceptance artifacts."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re

KEY_ALIASES = {
    "CPU\u53ca\u5185\u5b58": "CPU and memory",
    "P50\u7eb3\u79d2": "P50nanoseconds",
    "P95\u7eb3\u79d2": "P95nanoseconds",
    "P99\u590d\u6838\u7ebf": "P99review_line",
    "P99\u6bd4": "P99ratio",
    "P99\u7eb3\u79d2": "P99nanoseconds",
    "RMWP99\u7eb3\u79d2": "RMWP99nanoseconds",
    "RMW\u6302\u8d77\u6570": "RMWpending_count",
    "RMW\u64cd\u4f5c\u6570": "RMWoperation_count",
    "RSS\u5b57\u8282": "RSSbytes",
    "\u4e09\u8f6e\u4e2d\u4f4d\u6570": "Median of three rounds",
    "\u4e1a\u52a1\u540eRSS\u5b57\u8282": "after_businessRSSbytes",
    "\u4e1a\u52a1\u540e\u65e5\u5fd7\u8de8\u5ea6": "Post-business log span",
    "\u4e1a\u52a1\u540e\u7d2f\u8ba1\u5199\u5b57\u8282": "post_workload_write_bytes",
    "\u4e1a\u52a1\u540e\u7d2f\u8ba1\u8bfb\u5b57\u8282": "post_workload_read_bytes",
    "\u4e1a\u52a1\u540e\u9a7b\u7559\u9875\u5b57\u8282": "Post-transaction resident page bytes",
    "\u4e1a\u52a1\u64cd\u4f5c": "business operations",
    "\u4e2d\u4f4d\u6570": "median",
    "\u4e8c\u8fdb\u5236SHA256": "binarySHA256",
    "\u4ee3\u7801\u53d8\u5316": "code changes",
    "\u4fee\u6b63\u5dee\u5f02": "corrected differences",
    "\u5019\u9009": "candidate",
    "\u5019\u9009\u63d0\u4ea4": "candidate_commit",
    "\u5019\u9009\u6e90\u7801SHA256": "candidate_sourceSHA256",
    "\u503c": "value",
    "\u5185\u6838": "kernel",
    "\u5199\u5165P99\u7eb3\u79d2": "writeP99nanoseconds",
    "\u5199\u5165\u6302\u8d77\u6570": "writepending_count",
    "\u5199\u5165\u64cd\u4f5c\u6570": "writeoperation_count",
    "\u5199\u653e\u5927\u542b\u9884\u88c5\u68c0\u67e5\u70b9": "write_amplification_including_preload_checkpoint",
    "\u5206\u5e03": "distribution",
    "\u5206\u914d\u4e8b\u4ef6": "Assign events",
    "\u5206\u914d\u5b57\u8282": "allocate bytes",
    "\u5206\u914d\u5cf0\u503c\u5b57\u8282": "Allocate peak bytes",
    "\u5220\u9664P99\u7eb3\u79d2": "deleteP99nanoseconds",
    "\u5220\u9664\u6302\u8d77\u6570": "deletepending_count",
    "\u5220\u9664\u64cd\u4f5c\u6570": "deleteoperation_count",
    "\u539fP3\u56de\u9000\u9a8c\u6536": "original P3 rollback acceptance",
    "\u539fP3\u603b\u4f53\u56de\u9000": "original P3 overall rollback",
    "\u539fP3\u6570\u503c\u7ed3\u8bba": "original P3 numeric conclusion",
    "\u539fP3\u6574\u4f53\u56de\u9000": "original P3 overall rollback",
    "\u539f\u59cbP99\u590d\u6838\u7ebf": "originalP99review_line",
    "\u539f\u59cb\u541e\u5410\u590d\u6838\u7ebf": "Raw throughput review line",
    "\u539f\u59cb\u5dee\u5f02": "original difference",
    "\u539f\u59cb\u73af\u5883\u8bf4\u660e": "original environment",
    "\u539f\u6027\u80fd\u56de\u9000\u9a8c\u6536": "Original performance rollback acceptance",
    "\u539f\u751f\u8fd0\u884c": "native run",
    "\u53c2\u8003\u4fee\u6b63": "Reference correction",
    "\u53d8\u4f53": "variant",
    "\u540d\u79f0": "Name",
    "\u540e\u7aef": "backend",
    "\u541e\u5410\u4e2d\u4f4d\u6570": "Median throughput",
    "\u541e\u5410\u590d\u6838\u7ebf": "Throughput review line",
    "\u541e\u5410\u6bd4": "throughput_ratio",
    "\u547d\u4ee4": "command",
    "\u56fa\u5b9a\u8f93\u5165SHA256": "fixed input SHA256",
    "\u56fa\u5b9a\u8fb9\u754c": "fixed boundaries",
    "\u573a\u666f": "scene",
    "\u57fa\u7ebf": "baseline",
    "\u57fa\u7ebf\u63d0\u4ea4": "baseline_commit",
    "\u5893\u7891": "tombstone",
    "\u5b58\u50a8": "storage",
    "\u5b58\u6d3b\u5206\u914d\u5b57\u8282": "Survival allocated bytes",
    "\u5b58\u6d3b\u5206\u914d\u6570": "number of live allocations",
    "\u5b8c\u6574\u4e1a\u52a1\u5faa\u73af": "Complete business cycle",
    "\u5b8c\u6574\u573a\u666f": "complete scene",
    "\u5b8c\u6574\u77e9\u9635\u6bcf\u7aef\u6bcf\u7ec4\u6837\u672c": "Each set of samples at each end",
    "\u5b9e\u9645\u4e1a\u52a1\u64cd\u4f5c\u6570": "actual business operation count",
    "\u5de5\u5177\u94fe": "toolchain",
    "\u5de5\u5177\u94fe\u4e0e\u7279\u6027": "toolchain and features",
    "\u5e03\u5c40": "layout",
    "\u5e73\u53f0": "Platform",
    "\u5e94\u7528\u5199\u5165\u5b57\u8282": "application_write_bytes",
    "\u5f53\u524d\u4e0e\u65e7P3\u6210\u672c\u6bd4": "current_vs_oldP3cost_ratio",
    "\u5f53\u524d\u5b9e\u73b0": "Current implementation",
    "\u5f53\u524d\u63d0\u4ea4": "current commit",
    "\u6062\u590d\u503c\u6570": "Recovery value",
    "\u6062\u590d\u5199\u5b57\u8282": "Resume writing bytes",
    "\u6062\u590d\u540eRSS\u5b57\u8282": "after_recoveryRSSbytes",
    "\u6062\u590d\u5893\u7891\u6570": "Restoring the number of tombstones",
    "\u6062\u590d\u6821\u9a8c\u952e": "Restore check key",
    "\u6062\u590d\u6b21\u6570": "Recovery times",
    "\u6062\u590d\u7eb3\u79d2": "Recovery nanoseconds",
    "\u6062\u590d\u8bfb\u5b57\u8282": "Resume reading bytes",
    "\u6210\u529f": "success",
    "\u6267\u884c\u987a\u5e8f": "order",
    "\u62d2\u7edd\u91cd\u8bd5": "Deny retry",
    "\u6301\u4e45\u5316\u7d2f\u8ba1\u5199\u5b57\u8282": "Persistent accumulated written bytes",
    "\u6301\u4e45\u5316\u7d2f\u8ba1\u8bfb\u5b57\u8282": "Persistent accumulated read bytes",
    "\u6307\u6807": "indicator",
    "\u63a5\u53d7\u5e8f\u53f7": "Accept serial number",
    "\u64cd\u4f5c\u6570": "operation_count",
    "\u6574\u4f53P3\u56de\u9000\u9a8c\u6536": "overall P3 rollback acceptance",
    "\u6587\u4ef6SHA256": "fileSHA256",
    "\u6587\u4ef6\u63cf\u8ff0\u7b26": "file descriptor",
    "\u6587\u4ef6\u7cfb\u7edf": "file system",
    "\u65e7P3": "oldP3",
    "\u65e7P3\u63d0\u4ea4": "oldP3 commit",
    "\u65e7\u63d0\u4ea4": "old commit",
    "\u6700\u5927": "maximum",
    "\u6700\u5c0f": "minimum",
    "\u67b6\u6784": "architecture",
    "\u68c0\u67e5": "checks",
    "\u68c0\u67e5\u70b9\u540eRSS\u5b57\u8282": "after_checkpointRSSbytes",
    "\u68c0\u67e5\u70b9\u7eb3\u79d2": "checkpoint nanoseconds",
    "\u6a21\u5f0f": "mode",
    "\u6b63\u5e38\u9000\u51fa\u7801": "Normal exit code",
    "\u6b65\u6570": "number of steps",
    "\u6bcf\u4e2aCSV\u7a0b\u5e8f\u9000\u51fa\u7801": "each CSV program exit code",
    "\u6bcf\u64cd\u4f5c\u7eb3\u79d2\u4e2d\u4f4d\u6570": "median_nanoseconds_per_operation",
    "\u6bcf\u64cd\u4f5c\u8282\u7701\u7eb3\u79d2": "Nanoseconds saved per operation",
    "\u6bcf\u79d2\u64cd\u4f5c": "operations_per_second",
    "\u6bcf\u7aef\u6837\u672c": "samples_per_side",
    "\u6bcf\u7aef\u6bcf\u7ec4\u6837\u672c": "Each set of samples at each end",
    "\u6bcf\u7aef\u771f\u5b9e\u6837\u672c": "Real samples at each end",
    "\u6d4b\u91cf\u6761\u4ef6": "measurement conditions",
    "\u6d4b\u91cf\u8017\u65f6\u6beb\u79d2": "Measurement takes milliseconds",
    "\u70ed\u70b9\u8865\u5145\u6bcf\u8f6e\u6bcf\u7aef\u6837\u672c": "Real samples at each end",
    "\u7248\u672c": "version",
    "\u73af\u5883\u9650\u5236": "environment limits",
    "\u751f\u547d\u5468\u671f": "life cycle",
    "\u7528\u9014": "purpose",
    "\u771f\u5b9e\u6d4b\u8bd5": "real test",
    "\u79cd\u5b50": "seeds",
    "\u79fb\u9664\u5199\u5165\u5165\u53e3\u9636\u6bb5\u89c2\u5bdf": "Remove write entry phase observation",
    "\u79fb\u9664\u540e\u9000\u51fa\u7801": "Exit code after removal",
    "\u79fb\u9664\u6d3b\u52a8\u8bf7\u6c42\u8ba1\u6570": "Remove active request counting",
    "\u79fb\u9664\u7248\u672c\u8868\u767b\u8bb0\u4e0e\u7b49\u5f85": "Remove version table registration and waiting",
    "\u79fb\u9664\u90ae\u7bb1\u767b\u8bb0\u4e0e\u6ce8\u9500": "Remove email registration and logout",
    "\u7a0b\u5e8f": "program",
    "\u7a0b\u5e8f\u9000\u51fa\u7801": "program exit code",
    "\u7a97\u53e3\u5b57\u8282": "window_bytes",
    "\u7cfb\u7edf": "system",
    "\u7ebf\u7a0b": "threads",
    "\u7ed3\u679c": "result",
    "\u7ed3\u8bba": "conclusion",
    "\u7f3a\u5931": "missing",
    "\u8017\u65f6\u7eb3\u79d2": "elapsed_nanoseconds",
    "\u8865\u5145\u4e8c\u8fdb\u5236SHA256": "supplementary_binarySHA256",
    "\u89e6\u53d1\u590d\u6838": "trigger review",
    "\u8bc1\u636e\u8303\u56f4": "evidence scope",
    "\u8bf7\u6c42\u91c7\u6837\u79d2\u6570": "request sampling seconds",
    "\u8bfb\u53d6P99\u7eb3\u79d2": "readP99nanoseconds",
    "\u8bfb\u53d6\u6302\u8d77\u6570": "readpending_count",
    "\u8bfb\u53d6\u64cd\u4f5c\u6570": "readoperation_count",
    "\u8bfb\u53d6\u7ed3\u679c": "Read results",
    "\u8f6e\u6b21": "round",
    "\u8f93\u5165": "input",
    "\u8f93\u5165SHA256": "inputSHA256",
    "\u8f93\u5165\u6807\u8bc6": "input_id",
    "\u8fdb\u7a0b\u5185\u6bcf\u7ec4\u4e09\u8f6e": "three rounds per group in one process",
    "\u9000\u51fa\u7801": "exit_code",
    "\u91c7\u6837\u9000\u51fa\u7801": "sampling exit code",
    "\u91c7\u96c6\u65f6\u70b9": "collection time",
    "\u91c7\u96c6\u65f6\u70b9UTC": "collection time UTC",
    "\u91ca\u653e\u4e8b\u4ef6": "release_event",
    "\u952e\u6570": "Number of keys",
    "\u9650\u5236": "limit",
    "\u968f\u673a": "random",
    "\u9879\u76ee": "Project",
    "\u987a\u5e8f": "order",
    "\u9884\u70ed": "warmup",
    "\u9884\u88c5RSS\u5b57\u8282": "pre_installedRSSbytes",
    "\u9884\u88c5\u65e5\u5fd7\u8de8\u5ea6": "Preloaded log span",
    "\u9884\u88c5\u7d2f\u8ba1\u5199\u5b57\u8282": "Preloaded cumulative write bytes",
    "\u9884\u88c5\u7d2f\u8ba1\u8bfb\u5b57\u8282": "Preloaded cumulative read bytes",
    "\u9a71\u52a8SHA256": "driveSHA256"
}
KEY_ALIASES.update({
    "\u6821\u51c6AA": "calibrationAA",
    "\u590d\u6838AB": "reviewAB",
    "\u63d0\u4ea4": "commit",
    "\u5019\u9009\u8865\u4e01SHA256": "candidate_patchSHA256",
})
VALUE_ALIASES = {
    "256\u952e\u5747\u5300": "256uniform_keys",
    "Not completed": "Not completed",
    "P3\u603b\u4f53\u56de\u9000": "original P3 overall rollback",
    "miri-\u8bb0\u5f55\u751f\u547d\u5468\u671f.txt": "miri-record-lifetime.txt",
    "miri-\u9875\u5185\u503c\u5e03\u5c40.txt": "miri-in-page-value-layout.txt",
    "\u4e1a\u52a1\u901a\u8fc7\uff1b\u6027\u80fd\u89e6\u53d1\u539f\u590d\u6838\u7ebf\uff0c\u672a\u5b8c\u6210": "business passed; performance triggers the original review line; incomplete",
    "\u4ecd\u672a\u5b8c\u6210\uff1b\u957f\u91c7\u6837\u53ea\u590d\u6838\u672c\u5019\u9009\u4e0e f0f9725 \u7684\u53d8\u957f\u56db\u7ebf\u7a0b\u70ed\u70b9\u5dee\u5f02": "incomplete; long sampling only reviews the variable four-thread hotspot difference between this candidate and f0f9725",
    "\u5168\u90e8\u4e1a\u52a1\u6821\u9a8c\u901a\u8fc7\uff1b\u539f\u541e\u5410/P99\u89c4\u5219\u4ecd\u89e6\u53d1\u590d\u6838\uff0c\u9000\u51fa\u78011": "all business checks passed; original throughput/P99 rule still triggers review; exit code 1",
    "\u5199\u5165": "write",
    "\u5220\u9664": "delete",
    "\u5355\u952e\u70ed\u70b9": "single_key_hotspot",
    "\u539fP3\u603b\u4f53\u56de\u9000": "original P3 overall rollback",
    "\u539f\u5b50u64": "atomicu64",
    "\u53d8\u957f32\u81f3512\u5b57\u8282": "variable_length32to512bytes",
    "\u53d8\u957f\u5b57\u8282": "variable_bytes",
    "\u5747\u5300": "uniform",
    "\u590d\u6838AB": "reviewAB",
    "\u5931\u8d25": "failure",
    "\u5b8c\u6574": "complete",
    "\u6587\u4ef6\u7cfb\u7edf\uff1aext2/ext3": "file_system:ext2/ext3",
    "\u672a\u5b8c\u6210": "Not completed",
    "\u6821\u51c6AA": "calibrationAA",
    "\u6bcf\u7ebf\u7a0b\u70ed\u70b9": "per_thread_hotspot",
    "\u7eaf\u5185\u5b58": "memory_only",
    "\u8bfb\u53d6": "read",
    "\u8d85\u5185\u5b58": "over_memory",
    "\u901a\u8fc7": "passed",
    "\u90e8\u5206": "partial"
}
VALUE_ALIASES.update({
    "\u4e24\u4e2a\u6838\u5fc3\u4f7f\u7528\u5b8c\u5168\u76f8\u540c\u7684\u57fa\u51c6\u9a71\u52a8": "Both cores use the exact same benchmark driver",
    "\u4fdd\u5b58\u5168\u90e8\u539f\u59cb\u914d\u5bf9\u8bc1\u636e": "Save all original pairing evidence",
    "\u5176\u4ed6\u4f1a\u8bdd\u8f6e\u8be2\u540e\u7ed3\u679c\u7559\u5728\u539f\u90ae\u7bb1\u4e14\u9519\u8eab\u4efd\u4e0d\u80fd\u6536\u53d6": "after_other_session_polling_the_results_will_remain_in_the_original_mailbox_and_cannot_be_collected_if_the_identity_is_wrong",
    "\u9a8c\u8bc1\u6838\u5fc3\u8eab\u4efd\u5e76\u8bb0\u5f55\u539f\u751f\u73af\u5883": "Verify core identity and document native environment",
    "\u63d0\u4ea4\u548c\u8f6e\u8be2\u81ea\u52a8\u89c2\u5bdf\u7248\u672c\u4e14\u65e7\u8bf7\u6c42\u4fdd\u6301\u539f\u5207\u5206": "commits_and_polls_automatically_observe_versions_and_old_requests_remain_sharded",
    "\u65e7\u7248\u672c\u5168\u90e8\u9000\u51fa\u540e\u65b0\u7248\u672c\u624d\u53ef\u6267\u884c\u4e14\u4e0d\u540c\u952e\u4e92\u4e0d\u963b\u585e": "the_new_version_can_be_executed_only_after_all_old_versions_have_been_exited_and_different_keys_do_not_block_each_other",
    "\u9875\u5b57\u8282": "page bytes",
    "\u5185\u5b58\u9875": "memory page",
    "\u53ef\u53d8\u6bd4\u4f8b": "variable ratio",
    "\u7d22\u5f15\u6876": "index bucket",
    "\u7f13\u5b58\u542f\u7528": "Cache enabled",
    "\u7f13\u5b58\u5bb9\u91cf": "cache capacity",
    "\u81ea\u52a8\u538b\u7f29": "automatic_compression",
    "\u7ef4\u62a4\u5de5\u4f5c\u8005": "maintenance worker",
    "\u4f1a\u8bdd\u6570": "Number of sessions",
    "\u6302\u8d77\u9650\u989d": "pending limit",
    "\u7ed3\u679c\u9650\u989d": "result limit",
    "\u65e5\u5fd7\u9884\u5206\u914d": "Log pre-allocation",
    "\u6bb5\u5b57\u8282": "segment bytes",
})

def canonicalize(value):
    """Map historical localized keys and enum values to the current English schema."""
    if isinstance(value, dict):
        return {KEY_ALIASES.get(key, key): canonicalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [canonicalize(item) for item in value]
    if isinstance(value, str):
        for legacy, current in sorted(VALUE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            value = value.replace(legacy, current)
        return value
    return value

def read_json(path):
    return canonicalize(json.loads(Path(path).read_text()))

def read_rows(path):
    with Path(path).open(newline="") as stream:
        return [canonicalize(row) for row in csv.DictReader(stream)]

def canonicalize_text(text):
    """Normalize known localized labels in historical logs and filenames."""
    text = re.sub(
        r"\u57fa\u51c6 (\S+) \u901a\u8fc7\uff1a([0-9]+) \u6b65\u56db\u64cd\u4f5c\u3001([0-9]+) \u4e2a\u4f1a\u8bdd\u6062\u590d\u3001([0-9]+) \u4e2a\u952e\u4e0e\u5893\u7891\u6821\u9a8c",
        r"benchmark \1 passed:\2 steps_four_operations,\3 session resume,\4 keys_and_tombstone_validation",
        text,
    )
    for legacy, current in sorted(VALUE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        text = text.replace(legacy, current)
    return text

def resolve_path(root, relative):
    """Resolve an English path or its historical localized filename."""
    root = Path(root)
    candidate = root / relative
    if candidate.exists():
        return candidate
    expected = canonicalize_text(str(relative))
    for item in candidate.parent.iterdir():
        if canonicalize_text(item.name) == Path(expected).name:
            return item
    return candidate

//! 固定版本文本轨迹；字节使用十六进制，空字节使用短横线。
use std::fmt::Write;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Number(u64),
    Bytes(Vec<u8>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    Read {
        abort_if_tombstone: bool,
    },
    Upsert(Value),
    Rmw {
        operand: Value,
        create_if_missing: bool,
    },
    Delete {
        force_tombstone: bool,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub session: u64,
    pub serial: u64,
    pub key: Vec<u8>,
    pub operation: Operation,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trace {
    pub seed: u64,
    pub steps: Vec<Step>,
}
fn hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "-".into();
    }
    let mut result = String::new();
    for byte in bytes {
        write!(result, "{byte:02x}").unwrap();
    }
    result
}
fn unhex(text: &str) -> Result<Vec<u8>, String> {
    if text == "-" {
        return Ok(Vec::new());
    }
    if text.is_empty()
        || !text.len().is_multiple_of(2)
        || !text.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("无效十六进制".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| "无效字节".into()))
        .collect()
}
fn number(text: &str) -> Result<u64, String> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err("无效无符号整数".into());
    }
    text.parse().map_err(|_| "整数溢出".into())
}
fn flag(text: &str) -> Result<bool, String> {
    match text {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("标志必须为 0 或 1".into()),
    }
}
fn value(kind: &str, text: &str) -> Result<Value, String> {
    match kind {
        "u" => Ok(Value::Number(number(text)?)),
        "b" => Ok(Value::Bytes(unhex(text)?)),
        _ => Err("未知值类型".into()),
    }
}
fn encoded_value(value: &Value) -> String {
    match value {
        Value::Number(n) => format!("u {n}"),
        Value::Bytes(b) => format!("b {}", hex(b)),
    }
}
impl Trace {
    pub fn encode(&self) -> String {
        let mut out = format!("raster-trace 1 {}\n", self.seed);
        for step in &self.steps {
            let op = match &step.operation {
                Operation::Read { abort_if_tombstone } => {
                    format!("read {}", u8::from(*abort_if_tombstone))
                }
                Operation::Upsert(v) => format!("upsert {}", encoded_value(v)),
                Operation::Rmw {
                    operand,
                    create_if_missing,
                } => format!(
                    "rmw {} {}",
                    u8::from(*create_if_missing),
                    encoded_value(operand)
                ),
                Operation::Delete { force_tombstone } => {
                    format!("delete {}", u8::from(*force_tombstone))
                }
            };
            writeln!(
                out,
                "{} {} {} {op}",
                step.session,
                step.serial,
                hex(&step.key)
            )
            .unwrap();
        }
        out
    }
    pub fn decode(input: &str) -> Result<Self, String> {
        let mut lines = input.lines();
        let header: Vec<_> = lines
            .next()
            .ok_or("缺少轨迹头")?
            .split_whitespace()
            .collect();
        let ["raster-trace", "1", seed] = header.as_slice() else {
            return Err("未知轨迹版本或错误头部".into());
        };
        let mut trace = Self {
            seed: number(seed)?,
            steps: Vec::new(),
        };
        for (line, text) in lines.enumerate() {
            let fields: Vec<_> = text.split_whitespace().collect();
            let parsed = (|| {
                let [session, serial, key, rest @ ..] = fields.as_slice() else {
                    return Err("缺少操作字段".into());
                };
                let operation = match rest {
                    ["read", b] => Operation::Read {
                        abort_if_tombstone: flag(b)?,
                    },
                    ["upsert", k, v] => Operation::Upsert(value(k, v)?),
                    ["rmw", b, k, v] => Operation::Rmw {
                        operand: value(k, v)?,
                        create_if_missing: flag(b)?,
                    },
                    ["delete", b] => Operation::Delete {
                        force_tombstone: flag(b)?,
                    },
                    _ => return Err(String::from("操作字段不匹配")),
                };
                Ok(Step {
                    session: number(session)?,
                    serial: number(serial)?,
                    key: unhex(key)?,
                    operation,
                })
            })();
            trace
                .steps
                .push(parsed.map_err(|e| format!("第 {} 行：{e}", line + 2))?);
        }
        Ok(trace)
    }
    /// SplitMix64，显式 wrapping 运算使跨平台输出一致；每步固定消耗四个随机数。
    pub fn generate(seed: u64, count: usize) -> Self {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        };
        let mut serials = [0u64; 3];
        let mut steps = Vec::new();
        for _ in 0..count {
            let session = (next() % 3) as usize;
            let key_id = next() % 8;
            let choice = next();
            let payload = next();
            serials[session] = serials[session]
                .checked_add(1 + (choice % 3))
                .expect("测试轨迹过长");
            let key = if key_id == 0 {
                vec![]
            } else {
                vec![key_id as u8; key_id as usize]
            };
            let v = if key_id.is_multiple_of(2) {
                Value::Number(payload)
            } else {
                Value::Bytes(payload.to_le_bytes()[..(payload % 9) as usize].to_vec())
            };
            let operation = match choice % 4 {
                0 => Operation::Read {
                    abort_if_tombstone: choice & 4 != 0,
                },
                1 => Operation::Upsert(v),
                2 => Operation::Rmw {
                    operand: v,
                    create_if_missing: choice & 4 != 0,
                },
                _ => Operation::Delete {
                    force_tombstone: choice & 4 != 0,
                },
            };
            steps.push(Step {
                session: session as u64,
                serial: serials[session],
                key,
                operation,
            });
        }
        Self { seed, steps }
    }
}

use crate::columnar::{Cell, ColKind};
use crate::native::{NKind, NValue};

/// The root against which an expression is evaluated, plus the original record
/// text (for zero-copy `to_json_string($)`). For unwound rows an "element" is
/// overlaid as the `alias` key (e.g. `$.symbol`), while every other path
/// resolves against the parent record.
pub struct Ctx<'a, V: NValue> {
    pub base: &'a V,
    pub raw: &'a str,
    pub elem: Option<&'a V>,
    pub alias: Option<&'a str>,
}

impl<'a, V: NValue> Clone for Ctx<'a, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, V: NValue> Copy for Ctx<'a, V> {}

impl<'a, V: NValue> Ctx<'a, V> {
    pub fn report(root: &'a V, raw: &'a str) -> Self {
        Ctx { base: root, raw, elem: None, alias: None }
    }
    pub fn unwound(root: &'a V, raw: &'a str, elem: &'a V, alias: &'a str) -> Self {
        Ctx { base: root, raw, elem: Some(elem), alias: Some(alias) }
    }
}

/// A parsed field value, derived from a native value without ever materializing
/// a `serde_json::Value` DOM.
#[derive(Debug)]
pub enum LazyVal {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    /// Raw JSON text (for `to_json_string`).
    Raw(String),
}

impl LazyVal {
    pub fn is_null(&self) -> bool {
        matches!(self, LazyVal::Null)
    }

    /// Coerce this value to the target column kind (mirrors `cast(x as TYPE)`).
    pub fn to_cell(self, kind: ColKind) -> Cell {
        match kind {
            ColKind::Str => match self {
                LazyVal::Null => Cell::Null,
                LazyVal::Str(s) => Cell::Str(s),
                LazyVal::I64(i) => Cell::from(i),
                LazyVal::U64(u) => Cell::from(u.to_string()),
                LazyVal::F64(f) => Cell::from(f.to_string()),
                LazyVal::Bool(b) => Cell::from(if b { "true" } else { "false" }),
                LazyVal::Raw(r) => Cell::from(r),
            },
            ColKind::F64 => match self {
                LazyVal::Null => Cell::Null,
                LazyVal::F64(f) => Cell::from(f),
                LazyVal::I64(i) => Cell::from(i as f64),
                LazyVal::U64(u) => Cell::from(u as f64),
                LazyVal::Bool(b) => Cell::from(if b { 1.0 } else { 0.0 }),
                LazyVal::Str(s) => match s.trim().parse::<f64>() {
                    Ok(f) => Cell::from(f),
                    Err(_) => Cell::Null,
                },
                LazyVal::Raw(r) => match r.trim().parse::<f64>() {
                    Ok(f) => Cell::from(f),
                    Err(_) => Cell::Null,
                },
            },
            ColKind::I64 => match self {
                LazyVal::Null => Cell::Null,
                LazyVal::I64(i) => Cell::from(i),
                LazyVal::U64(u) => Cell::from(u as i64),
                LazyVal::F64(f) => Cell::from(f as i64),
                LazyVal::Bool(b) => Cell::from(if b { 1 } else { 0 }),
                LazyVal::Str(s) => match s.trim().parse::<i64>() {
                    Ok(i) => Cell::from(i),
                    Err(_) => Cell::Null,
                },
                LazyVal::Raw(r) => match r.trim().parse::<i64>() {
                    Ok(i) => Cell::from(i),
                    Err(_) => Cell::Null,
                },
            },
            ColKind::Bool => match self {
                LazyVal::Null => Cell::Null,
                LazyVal::Bool(b) => Cell::from(b),
                LazyVal::I64(i) => Cell::from(i != 0),
                LazyVal::U64(u) => Cell::from(u != 0),
                LazyVal::F64(f) => Cell::from(f != 0.0),
                LazyVal::Str(s) => match s.trim() {
                    "true" | "True" | "1" => Cell::from(true),
                    "false" | "False" | "0" => Cell::from(false),
                    _ => Cell::Null,
                },
                LazyVal::Raw(r) => match r.trim() {
                    "true" | "True" | "1" => Cell::from(true),
                    "false" | "False" | "0" => Cell::from(false),
                    _ => Cell::Null,
                },
            },
        }
    }
}

/// A compiled expression node, evaluated lazily per record.
#[derive(Debug)]
pub enum Node {
    Root,
    Path(Vec<Seg>),
    Lit(LazyLit),
    Coalesce(Vec<Node>),
    Cast(Box<Node>, ColKind),
    ToJson(Box<Node>),
}

#[derive(Debug)]
pub enum Seg {
    Key(String),
    Idx(usize),
}

#[derive(Debug, Clone)]
pub enum LazyLit {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
}

/// Compiles an ETL field expression into a `Node` tree (fast reuse per record).
pub fn compile(expr: &str) -> Node {
    let mut c = Compiler {
        chars: expr.trim().chars().collect(),
        pos: 0,
    };
    c.p_expr().unwrap_or(Node::Lit(LazyLit::Null))
}

/// Evaluates a compiled `Node` against a native context, lazily (no DOM).
pub fn eval_node<V: NValue>(n: &Node, ctx: &Ctx<'_, V>) -> LazyVal {
    match n {
        Node::Root => LazyVal::Raw(ctx.raw.to_string()),
        Node::Path(segs) => {
            let mut cur: &V = ctx.base;
            let mut failed = false;
            let mut first = true;
            for seg in segs {
                if failed {
                    continue;
                }
                let alias_hit = match seg {
                    Seg::Key(k) => first && ctx.alias.map(|a| a == k).unwrap_or(false),
                    Seg::Idx(_) => false,
                };
                if alias_hit {
                    cur = ctx.elem.unwrap_or(ctx.base);
                } else {
                    match descend_seg(cur, seg) {
                        Some(v) => cur = v,
                        None => failed = true,
                    }
                }
                first = false;
            }
            if failed {
                LazyVal::Null
            } else {
                to_lazy(cur, ctx)
            }
        }
        Node::Lit(l) => match l {
            LazyLit::Null => LazyVal::Null,
            LazyLit::Bool(b) => LazyVal::Bool(*b),
            LazyLit::I64(i) => LazyVal::I64(*i),
            LazyLit::F64(f) => LazyVal::F64(*f),
            LazyLit::Str(s) => LazyVal::Str(s.clone()),
        },
        Node::Coalesce(args) => {
            for a in args {
                let v = eval_node(a, ctx);
                if !v.is_null() {
                    return v;
                }
            }
            LazyVal::Null
        }
        Node::Cast(inner, kind) => eval_node(inner, ctx).to_cell(*kind).into_lazy(),
        Node::ToJson(inner) => LazyVal::Str(json_of(&eval_node(inner, ctx))),
    }
}

fn descend_seg<'v, V: NValue>(node: &'v V, seg: &Seg) -> Option<&'v V> {
    match seg {
        Seg::Idx(i) => node.get_idx(*i),
        Seg::Key(k) => {
            if let Ok(i) = k.parse::<usize>() {
                if node.is_array() {
                    return node.get_idx(i);
                }
                return None;
            }
            node.get(k)
        }
    }
}

/// Convert a native value to a `LazyVal` (no DOM build).
fn to_lazy<V: NValue>(v: &V, ctx: &Ctx<'_, V>) -> LazyVal {
    match v.kind() {
        NKind::Null => LazyVal::Null,
        NKind::Bool => LazyVal::Bool(v.as_bool().unwrap_or(false)),
        NKind::Int => match v.as_i64() {
            Some(i) => LazyVal::I64(i),
            None => LazyVal::U64(v.as_u64().unwrap_or(0)),
        },
        NKind::UInt => LazyVal::U64(v.as_u64().unwrap_or(0)),
        NKind::Float => LazyVal::F64(v.as_f64().unwrap_or(0.0)),
        NKind::Str => LazyVal::Str(v.as_str().unwrap_or("").to_string()),
        NKind::Object | NKind::Array => {
            if is_root(v, ctx) {
                LazyVal::Raw(ctx.raw.to_string())
            } else if let Some(t) = v.raw_text() {
                LazyVal::Raw(t)
            } else {
                LazyVal::Null
            }
        }
    }
}

fn is_root<V: NValue>(v: &V, ctx: &Ctx<'_, V>) -> bool {
    std::ptr::eq(v as *const V, ctx.base as *const V)
}

struct Compiler {
    chars: Vec<char>,
    pos: usize,
}

impl Compiler {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }
    fn starts_with(&mut self, s: &str) -> bool {
        self.skip_ws();
        let rem: Vec<char> = self.chars[self.pos..].to_vec();
        let t: Vec<char> = s.chars().collect();
        rem.len() >= t.len() && rem[..t.len()] == t
    }
    fn step(&mut self, n: usize) {
        self.skip_ws();
        self.pos += n;
    }
    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> String {
        self.skip_ws();
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.pos += 1;
            } else {
                break;
            }
        }
        s
    }

    fn p_expr(&mut self) -> Result<Node, ()> {
        self.skip_ws();
        if self.starts_with("cast(") {
            self.step(5);
            let inner = self.p_expr()?;
            let _as = self.ident();
            let ty = self.ident().to_lowercase();
            let _ = self.eat(')');
            return Ok(Node::Cast(Box::new(inner), cast_type(&ty)));
        }
        if self.starts_with("to_json_string(") {
            self.step(15);
            let inner = self.p_expr()?;
            let _ = self.eat(')');
            return Ok(Node::ToJson(Box::new(inner)));
        }
        if self.starts_with("coalesce(") {
            self.step(9);
            let mut args = Vec::new();
            loop {
                let v = self.p_expr()?;
                args.push(v);
                if self.eat(',') {
                    continue;
                }
                break;
            }
            let _ = self.eat(')');
            return Ok(Node::Coalesce(args));
        }
        self.p_atom()
    }

    fn p_atom(&mut self) -> Result<Node, ()> {
        self.skip_ws();
        match self.peek() {
            None => Ok(Node::Lit(LazyLit::Null)),
            Some('$') => {
                self.pos += 1;
                let mut segs = Vec::new();
                loop {
                    self.skip_ws();
                    match self.peek() {
                        Some('.') => {
                            self.pos += 1;
                            let seg = self.seg_ident();
                            if !seg.is_empty() {
                                segs.push(Seg::Key(seg));
                            }
                        }
                        Some('[') => {
                            self.pos += 1;
                            let mut num = String::new();
                            while let Some(c) = self.peek() {
                                if c == ']' {
                                    self.pos += 1;
                                    break;
                                }
                                num.push(c);
                                self.pos += 1;
                            }
                            if let Ok(i) = num.parse::<usize>() {
                                segs.push(Seg::Idx(i));
                            }
                        }
                        _ => break,
                    }
                }
                if segs.is_empty() {
                    Ok(Node::Root)
                } else {
                    Ok(Node::Path(segs))
                }
            }
            Some('\'') => {
                self.pos += 1;
                let mut s = String::new();
                while let Some(c) = self.peek() {
                    self.pos += 1;
                    if c == '\'' {
                        break;
                    }
                    if c == '\\' {
                        if let Some(n) = self.peek() {
                            s.push(n);
                            self.pos += 1;
                        }
                    } else {
                        s.push(c);
                    }
                }
                Ok(Node::Lit(LazyLit::Str(s)))
            }
            Some('t') => {
                let id = self.ident();
                Ok(Node::Lit(LazyLit::Bool(id == "true")))
            }
            Some('f') => {
                let _ = self.ident();
                Ok(Node::Lit(LazyLit::Bool(false)))
            }
            Some('n') => {
                let _ = self.ident();
                Ok(Node::Lit(LazyLit::Null))
            }
            _ => {
                let mut s = String::new();
                while let Some(d) = self.peek() {
                    if d.is_ascii_digit() || d == '.' || d == '-' || d == 'e' || d == 'E' || d == '+' {
                        s.push(d);
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                if s.is_empty() {
                    Ok(Node::Lit(LazyLit::Null))
                } else if s.contains('.') || s.contains('e') || s.contains('E') {
                    Ok(Node::Lit(LazyLit::F64(s.parse().unwrap_or(0.0))))
                } else {
                    Ok(Node::Lit(LazyLit::I64(s.parse().unwrap_or(0))))
                }
            }
        }
    }

    fn seg_ident(&mut self) -> String {
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if is_seg_stop(c) {
                break;
            }
            s.push(c);
            self.pos += 1;
        }
        s
    }
}

/// Evaluates a (raw, uncompiled) ETL expression against a native root, lazily.
pub fn eval_lazy<V: NValue>(expr: &str, ctx: Ctx<'_, V>) -> LazyVal {
    let node = compile(expr);
    eval_node(&node, &ctx)
}

/// Lazy evaluation of an unwind condition: `PATH CONTAINS 'TXT'` / `= 'V'` /
/// bare literal.
pub fn eval_condition_lazy<V: NValue>(cond: &str, ctx: Ctx<'_, V>) -> bool {
    let lower = cond.trim().to_lowercase();
    if let Some(ci) = lower.find("contains") {
        let pre_ok = cond[..ci].trim();
        let post = &cond[ci + "contains".len()..].trim();
        let l = eval_lazy(pre_ok, ctx);
        let r = eval_lazy(post, ctx);
        if let (LazyVal::Str(a), LazyVal::Str(b)) = (l, r) {
            return a.contains(&b);
        }
        return false;
    }
    if let Some(idx) = cond.find('=') {
        let l = eval_lazy(cond[..idx].trim(), ctx);
        let r = eval_lazy(cond[idx + 1..].trim(), ctx);
        return lazy_eq(&l, &r);
    }
    match eval_lazy(cond.trim(), ctx) {
        LazyVal::Bool(b) => b,
        LazyVal::Null => false,
        _ => true,
    }
}

fn lazy_eq(a: &LazyVal, b: &LazyVal) -> bool {
    use LazyVal::*;
    match (a, b) {
        (Str(x), Str(y)) => x == y,
        (I64(x), I64(y)) => x == y,
        (F64(x), F64(y)) => x == y,
        (Bool(x), Bool(y)) => x == y,
        (Str(x), I64(y)) => x.parse::<i64>().ok() == Some(*y),
        (I64(x), Str(y)) => y.parse::<i64>().ok() == Some(*x),
        (Str(x), F64(y)) => x.trim().parse::<f64>().ok() == Some(*y),
        (F64(x), Str(y)) => y.trim().parse::<f64>().ok() == Some(*x),
        _ => false,
    }
}

fn cast_type(t: &str) -> ColKind {
    match t {
        "double" | "float" | "decimal" => ColKind::F64,
        "integer" | "int" | "long" => ColKind::I64,
        "string" => ColKind::Str,
        _ => ColKind::Str,
    }
}

/// Infer the column kind a field expression will produce (based on its `cast`).
pub fn expr_kind(expr: &str) -> ColKind {
    let lower = expr.to_lowercase();
    if let Some(ci) = lower.find(" as ") {
        let rest = &expr[ci + 4..];
        let tok: String =
            rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
        return cast_type(&tok.to_lowercase());
    }
    if ["true", "false"].iter().any(|t| lower.split(['(', ')', ',', ' ']).any(|w| w == *t)) {
        return ColKind::Bool;
    }
    ColKind::Str
}

/// If `expr` is a joined-column projection `<alias>.<col>` (optionally wrapped
/// in `cast(... as TYPE)`), returns `(col, kind)`. Used to source a field from a
/// materialized join table rather than from the default JSON source.
pub fn joined_projection(alias: &str, expr: &str) -> Option<(String, ColKind)> {
    let e = expr.trim();
    if e.is_empty() {
        return None;
    }
    let kind = expr_kind(e);
    let inner = if let Some(rest) = e.strip_prefix("cast(") {
        let stripped = rest.strip_suffix(')').unwrap_or(rest);
        match stripped.rfind(" as ") {
            Some(i) => &stripped[..i],
            None => stripped,
        }
    } else {
        e
    };
    let inner = inner.trim();
    let prefix = format!("{}.", alias);
    if let Some(rest) = inner.strip_prefix(&prefix) {
        let col = rest.trim().split('.').next().unwrap_or("").to_string();
        if !col.is_empty() {
            return Some((col, kind));
        }
    }
    None
}


/// Resolves a full JSON path (e.g. `$.KIKOSelect.underlying`) natively, returning
/// a reference to the node — used to get the unwind array elements.
pub fn resolve_path<'v, V: NValue>(ctx: Ctx<'v, V>, path: &str) -> Option<&'v V> {
    let p = path.trim().strip_prefix('$')?;
    let mut cur: &'v V = ctx.base;
    let mut chars = p.chars().peekable();
    let mut is_first = true;
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '[' {
            let mut seg = String::new();
            for ch in chars.by_ref() {
                if ch == ']' {
                    break;
                }
                seg.push(ch);
            }
            cur = descend(cur, &seg, ctx)?;
            is_first = false;
        } else if c == '.' {
            let mut seg = String::new();
            while let Some(&ch) = chars.peek() {
                if is_seg_stop(ch) {
                    break;
                }
                seg.push(ch);
                chars.next();
            }
            cur = step(cur, &seg, ctx, is_first)?;
            is_first = false;
        } else {
            let mut seg = c.to_string();
            while let Some(&ch) = chars.peek() {
                if is_seg_stop(ch) {
                    break;
                }
                seg.push(ch);
                chars.next();
            }
            cur = step(cur, &seg, ctx, is_first)?;
            is_first = false;
        }
    }
    Some(cur)
}

fn step<'v, V: NValue>(cur: &'v V, seg: &str, ctx: Ctx<'v, V>, is_first: bool) -> Option<&'v V> {
    if is_first && ctx.alias.map(|a| a == seg).unwrap_or(false) {
        return ctx.elem.or(Some(ctx.base));
    }
    descend(cur, seg, ctx)
}

fn descend<'v, V: NValue>(node: &'v V, seg: &str, _ctx: Ctx<'_, V>) -> Option<&'v V> {
    if seg.is_empty() {
        return Some(node);
    }
    if let Ok(idx) = seg.parse::<usize>() {
        if node.is_array() {
            return node.get_idx(idx);
        }
        return None;
    }
    if node.is_object() {
        return node.get(seg);
    }
    None
}

fn json_of(v: &LazyVal) -> String {
    match v {
        LazyVal::Raw(r) => r.clone(),
        LazyVal::Str(s) => format!("\"{}\"", s),
        LazyVal::I64(i) => i.to_string(),
        LazyVal::U64(u) => u.to_string(),
        LazyVal::F64(f) => f.to_string(),
        LazyVal::Bool(b) => b.to_string(),
        LazyVal::Null => "null".to_string(),
    }
}

fn is_seg_stop(c: char) -> bool {
    c == '.' || c == '[' || c == ',' || c == '(' || c == ')' || c.is_whitespace()
}

trait IntoLazy {
    fn into_lazy(self) -> LazyVal;
}
impl IntoLazy for Cell {
    fn into_lazy(self) -> LazyVal {
        match self {
            Cell::Null => LazyVal::Null,
            Cell::Str(s) => LazyVal::Str(s),
            Cell::F64(f) => LazyVal::F64(f),
            Cell::I64(i) => LazyVal::I64(i),
            Cell::Bool(b) => LazyVal::Bool(b),
        }
    }
}

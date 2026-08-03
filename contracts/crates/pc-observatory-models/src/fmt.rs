use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt::{Debug, Display, Write};

pub(crate) struct TruncatedList<I> {
    iter: RefCell<I>,
    max_elems: usize,
}

struct DebugDisplay<T>(T);
impl<T: Display> Debug for DebugDisplay<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub(crate) fn display<T: Display>(value: T) -> impl Debug {
    DebugDisplay(value)
}

impl<I, T> Display for TruncatedList<I>
where
    I: Iterator<Item = T>,
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.iter.borrow_mut();
        let mut list = f.debug_list();
        list.entries(iter.by_ref().take(self.max_elems).map(display));
        if iter.next().is_some() {
            list.finish_non_exhaustive()
        } else {
            list.finish()
        }
    }
}

pub(crate) fn display_trunc<I: IntoIterator<Item = T>, T: Display>(
    value: I,
    max_elems: usize,
) -> TruncatedList<I::IntoIter> {
    TruncatedList {
        iter: value.into_iter().into(),
        max_elems,
    }
}

pub(crate) struct PrefixWrite<'a, W> {
    indent_level: usize,
    indent_size: usize,
    inner: &'a mut W,
    last_was_newline: bool,
    prev_level: usize,
}

impl<'a, W> PrefixWrite<'a, W> {
    pub(crate) fn new(inner: &'a mut W, indent_size: usize) -> Self {
        Self {
            inner,
            indent_size,
            indent_level: 0,
            last_was_newline: true,
            prev_level: 0,
        }
    }

    pub(crate) fn set_indent_level(&mut self, level: usize) {
        self.prev_level = self.indent_level;
        self.indent_level = level;
    }

    pub(crate) fn with_indent(
        &mut self,
        level: usize,
        mut f: impl FnMut(&mut Self) -> std::fmt::Result,
    ) -> std::fmt::Result {
        let initial = self.indent_level;
        self.set_indent_level(level);
        let res = f(self);
        self.set_indent_level(initial);
        res
    }
}

impl<W> Write for PrefixWrite<'_, W>
where
    W: Write,
{
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.indent_level != self.prev_level && !self.last_was_newline {
            self.prev_level = self.indent_level;
            write!(self, "\n{s}")?;
            return Ok(());
        }
        self.prev_level = self.indent_level;
        let mut lines = s.split_inclusive('\n');
        if !self.last_was_newline
            && let Some(first_line) = lines.next()
        {
            self.inner.write_str(first_line)?;
        }
        for line in lines {
            write!(
                self.inner,
                "{:indent$}{line}",
                "",
                indent = self.indent_level * self.indent_size
            )?;
        }
        if let Some(c) = s.chars().last() {
            self.last_was_newline = c == '\n';
        }
        Ok(())
    }
}

pub(crate) enum StackItem {
    Node(usize),
    String(Cow<'static, str>),
}

pub(crate) struct FmtStack<'a> {
    stack: Vec<(StackItem, usize)>,
    adj: &'a Vec<Vec<usize>>,
}

impl<'a> FmtStack<'a> {
    pub(crate) fn new(adj: &'a Vec<Vec<usize>>, n_roots: usize) -> Self {
        Self {
            stack: (0..n_roots)
                .map(|value| (StackItem::Node(value), 0))
                .collect(),
            adj,
        }
    }

    pub(crate) fn add_sources(&mut self, idx: usize, level: usize) {
        self.stack.extend(
            self.adj[idx]
                .iter()
                .rev()
                .copied()
                .map(|value| (StackItem::Node(value), level)),
        );
    }

    pub(crate) fn add(&mut self, nodes: impl DoubleEndedIterator<Item = (StackItem, usize)>) {
        self.stack.extend(nodes.rev());
    }

    pub(crate) fn pop(&mut self) -> Option<(StackItem, usize)> {
        self.stack.pop()
    }
}

struct IterAsSlice<I>(RefCell<I>);

pub(crate) fn iter_as_slice<T: Display, I: Iterator<Item = T>>(iter: I) -> impl Display {
    IterAsSlice(iter.into())
}

impl<T: Display, I: Iterator<Item = T>> Display for IterAsSlice<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.borrow_mut().by_ref().map(display))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    #[test]
    fn test_indent_writer() {
        let mut s = String::new();
        let mut w = PrefixWrite::new(&mut s, 2);

        writeln!(w, "Hello\nWorld").unwrap();
        assert_eq!(w.inner, "Hello\nWorld\n");

        w.set_indent_level(1);
        w.inner.clear();
        write!(w, "Hello\nWorld").unwrap();
        w.set_indent_level(2);
        write!(w, "Sub").unwrap();
        assert_eq!(w.inner, "  Hello\n  World\n    Sub");
    }
}

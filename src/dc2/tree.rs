use crate::common::{
    error::{FloppyError, Result},
    ivec::IVec,
};
use crate::dc2::codec::Codec;
use crate::dc2::node::insert_leaf_node;
use crate::dc2::{
    buf::{LockGuard, PinGuard},
    bufmgr::BufMgr,
    codec::Record,
    meta::MetaPage,
    node::{compare_high_key, find_child, Node},
};
use crate::env::Env;
use std::cmp::Ordering;
use std::path::Path;

pub(crate) struct Tree<E: Env> {
    buf_mgr: BufMgr<E>,
}

impl<E> Tree<E>
where
    E: Env,
{
    pub async fn open<P: AsRef<Path>>(path: P, env: E) -> Result<Self> {
        let buf_mgr = BufMgr::open(env, path, 1000).await?;
        Ok(Self { buf_mgr })
    }

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        todo!()
    }

    pub async fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let (mut lock_guard, stack) = self.find_leaf(key.as_ref()).await?;
        let record = Record {
            key: key.as_ref(),
            value: value.as_ref(),
        };

        let mut node = Node::from_page(&mut lock_guard.page);

        if node.will_overfull(record.encode_size()) {
            // need split
            todo!()
        } else {
            insert_leaf_node(&mut node, record)
        }
    }

    async fn find_leaf(
        &self,
        key: &[u8],
    ) -> Result<(LockGuard, Vec<PinGuard>)> {
        let mut lock_guard = self.get_root().await?.lock();
        let mut stack = vec![];
        loop {
            lock_guard = self.move_right(key, lock_guard).await?;
            let node = Node::from_page(&mut lock_guard.page);
            if node.is_leaf() {
                return Ok((lock_guard, stack));
            } else {
                let page_id = find_child(&node, key)?;
                // release parent's lock and get the parent's pin.
                let parent_pin = lock_guard.unlock();
                stack.push(parent_pin);

                // lock the child page
                lock_guard = self.buf_mgr.fix_page(page_id).await?.lock();
            }
        }
    }

    async fn split(
        &self,
        node: &mut Node<'_>,
        record: Record<'_, &[u8]>,
    ) -> Result<()> {
        todo!()
    }

    async fn move_right(
        &self,
        key: &[u8],
        mut lock_guard: LockGuard,
    ) -> Result<LockGuard> {
        loop {
            let node = Node::from_page(&mut lock_guard.page);
            if compare_high_key(&node, key) == Ordering::Greater {
                let page_id = node.get_right_sibling();
                drop(lock_guard);
                let pin_guard = self.buf_mgr.fix_page(page_id).await?;
                lock_guard = pin_guard.lock()
            } else {
                return Ok(lock_guard);
            }
        }
    }

    async fn get_root(&self) -> Result<PinGuard> {
        let pin_guard = self.buf_mgr.fix_page(0).await?;
        let mut lock_guard = pin_guard.lock();
        let page = &mut lock_guard.page;
        let meta_page = MetaPage::from_page(page);
        let root_id = meta_page.get_root();
        if root_id == 0 {
            let pin_guard = self.buf_mgr.alloc_page().await?;
            let mut lock_guard = pin_guard.lock();
            let mut node = Node::from_page(&mut lock_guard.page);
            node.format_page();
            Ok(pin_guard)
        } else {
            self.buf_mgr.fix_page(root_id).await
        }
    }
}

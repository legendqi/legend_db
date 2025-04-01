use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::BufRead;

const ORDER: usize = 5; // B+树的阶数

// B+树中的键值对
#[derive(Debug)]
struct KeyValue<K, V> {
    key: K,
    value: V,
}

// B+树节点
enum BPlusTreeNode<K, V> {
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

// 内部节点
struct InternalNode<K, V> {
    keys: Vec<K>,
    children: Vec<Box<BPlusTreeNode<K, V>>>,
}

// 叶子节点
struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    next: Option<Box<LeafNode<K, V>>>,
}

// B+树
pub struct BPlusTree<K, V> {
    root: Option<Box<BPlusTreeNode<K, V>>>,
    length: usize,
}

impl<K: Ord + Clone + Debug, V: Debug> BPlusTree<K, V> {
    /// 创建一个新的空B+树
    pub fn new() -> Self {
        BPlusTree {
            root: None,
            length: 0,
        }
    }

    /// 插入键值对
    pub fn insert(&mut self, key: K, value: V) {
        if let Some(root) = &mut self.root {
            // 如果根节点已满，需要分裂
            if root.is_full() {
                let mut old_root = std::mem::replace(root, Box::new(BPlusTreeNode::new_internal()));
                let (new_key, new_child) = old_root.split();
                if let BPlusTreeNode::Internal(internal) = &mut **root {
                    internal.keys.push(new_key);
                    internal.children.push(old_root);
                    internal.children.push(new_child);
                }
            }

            // 从根节点开始插入
            if let Some((new_key, new_child)) = root.insert(key, value) {
                // 处理根节点分裂的情况
                let mut new_root = Box::new(BPlusTreeNode::new_internal());
                if let BPlusTreeNode::Internal(internal) = &mut *new_root {
                    internal.keys.push(new_key);
                    internal.children.push(std::mem::replace(root, Box::new(BPlusTreeNode::new_leaf())));
                    internal.children.push(new_child);
                    *root = new_root;
                }
            }
        } else {
            // 树为空，创建新的叶子节点作为根
            let mut leaf = Box::new(BPlusTreeNode::new_leaf());
            leaf.insert(key, value);
            self.root = Some(leaf);
        }
        self.length += 1;
    }

    /// 查找键对应的值
    pub fn get(&self, key: &K) -> Option<&V> {
        self.root.as_ref().and_then(|root| root.get(key))
    }

    /// 删除键对应的值
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(root) = &mut self.root {
            let result = root.remove(key);
            if result.is_some() {
                self.length -= 1;
            }

            // 如果根节点是内部节点且只有一个子节点，则降低树的高度
            if let BPlusTreeNode::Internal(internal) = &mut **root {
                if internal.children.len() == 1 {
                    let only_child = internal.children.remove(0);
                    *root = only_child;
                }
            }

            result
        } else {
            None
        }
    }

    /// 返回树中元素的数量
    pub fn len(&self) -> usize {
        self.length
    }

    /// 判断树是否为空
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl<K: Ord + Clone + Debug, V: Debug> BPlusTreeNode<K, V> {
    /// 创建一个新的内部节点
    fn new_internal() -> Self {
        BPlusTreeNode::Internal(InternalNode {
            keys: Vec::with_capacity(ORDER),
            children: Vec::with_capacity(ORDER + 1),
        })
    }

    /// 创建一个新的叶子节点
    fn new_leaf() -> Self {
        BPlusTreeNode::Leaf(LeafNode {
            keys: Vec::with_capacity(ORDER),
            values: Vec::with_capacity(ORDER),
            next: None,
        })
    }

    /// 判断节点是否已满
    fn is_full(&self) -> bool {
        match self {
            BPlusTreeNode::Internal(internal) => internal.keys.len() >= ORDER - 1,
            BPlusTreeNode::Leaf(leaf) => leaf.keys.len() >= ORDER,
        }
    }

    /// 插入键值对
    fn insert(&mut self, key: K, value: V) -> Option<(K, Box<BPlusTreeNode<K, V>>)> {
        match self {
            BPlusTreeNode::Internal(internal) => {
                // 找到合适的子节点进行插入
                let idx = internal.find_child_idx(&key);
                let child = &mut internal.children[idx];

                if child.is_full() {
                    // 子节点已满，需要分裂
                    let (new_key, new_child) = child.split();
                    internal.keys.insert(idx, new_key);
                    internal.children.insert(idx + 1, new_child);

                    // 重新确定插入位置
                    if key > internal.keys[idx] {
                        internal.children[idx + 1].insert(key, value)
                    } else {
                        internal.children[idx].insert(key, value)
                    }
                } else {
                    child.insert(key, value)
                }
            }
            BPlusTreeNode::Leaf(leaf) => {
                // 找到插入位置
                let idx = leaf.keys.binary_search(&key).unwrap_or_else(|x| x);
                leaf.keys.insert(idx, key);
                leaf.values.insert(idx, value);

                // 检查是否需要分裂
                if leaf.keys.len() > ORDER {
                    let split_at = leaf.keys.len() / 2;
                    let split_key = leaf.keys[split_at].clone();

                    // 创建新叶子节点
                    let mut new_leaf = Box::new(BPlusTreeNode::new_leaf());
                    if let BPlusTreeNode::Leaf(new_leaf_node) = &mut *new_leaf.clone() {
                        new_leaf_node.keys = leaf.keys.drain(split_at..).collect();
                        new_leaf_node.values = leaf.values.drain(split_at..).collect();
                        new_leaf_node.next = leaf.next.take();
                        leaf.next = Some(new_leaf);
                    }

                    Some((split_key, new_leaf))
                } else {
                    None
                }
            }
        }
    }

    /// 分裂节点
    fn split(&mut self) -> (K, Box<BPlusTreeNode<K, V>>) {
        match self {
            BPlusTreeNode::Internal(internal) => {
                let split_at = internal.keys.len() / 2;
                let split_key = internal.keys[split_at].clone();

                let mut new_internal = Box::new(BPlusTreeNode::new_internal());
                if let BPlusTreeNode::Internal(new_node) = &mut *new_internal {
                    new_node.keys = internal.keys.drain(split_at + 1..).collect();
                    new_node.children = internal.children.drain(split_at + 1..).collect();
                }

                (split_key, new_internal)
            }
            BPlusTreeNode::Leaf(leaf) => {
                let split_at = leaf.keys.len() / 2;
                let split_key = leaf.keys[split_at].clone();

                let mut new_leaf = Box::new(BPlusTreeNode::new_leaf());
                if let BPlusTreeNode::Leaf(new_leaf_node) = &mut *new_leaf {
                    new_leaf_node.keys = leaf.keys.drain(split_at..).collect();
                    new_leaf_node.values = leaf.values.drain(split_at..).collect();
                    new_leaf_node.next = leaf.next.take();
                    leaf.next = Some(new_leaf);
                }

                (split_key, new_leaf)
            }
        }
    }

    /// 查找键对应的值
    fn get(&self, key: &K) -> Option<&V> {
        match self {
            BPlusTreeNode::Internal(internal) => {
                let idx = internal.find_child_idx(key);
                internal.children[idx].get(key)
            }
            BPlusTreeNode::Leaf(leaf) => {
                leaf.keys.binary_search(key)
                    .ok()
                    .map(|idx| &leaf.values[idx])
            }
        }
    }

    /// 删除键对应的值
    fn remove(&mut self, key: &K) -> Option<V> {
        match self {
            BPlusTreeNode::Internal(internal) => {
                let idx = internal.find_child_idx(key);
                let result = internal.children[idx].remove(key);

                // TODO: 处理节点合并/重新分配的情况
                result
            }
            BPlusTreeNode::Leaf(leaf) => {
                if let Ok(idx) = leaf.keys.binary_search(key) {
                    leaf.keys.remove(idx);
                    Some(leaf.values.remove(idx))
                } else {
                    None
                }
            }
        }
    }
}

impl<K: Ord, V> InternalNode<K, V> {
    /// 找到合适的子节点索引
    fn find_child_idx(&self, key: &K) -> usize {
        match self.keys.binary_search(key) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    }
}



//chatgpt
use std::collections::BTreeMap;
use std::rc::Rc;
use std::cell::RefCell;

const ORDER: usize = 4; // B+树的阶数

type NodeRef<K, V> = Rc<RefCell<Node<K, V>>>;

#[derive(Debug)]
enum Node<K, V> {
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

#[derive(Debug)]
struct InternalNode<K, V> {
    keys: Vec<K>,
    children: Vec<NodeRef<K, V>>,
}

#[derive(Debug)]
struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    next: Option<NodeRef<K, V>>, // 指向下一个叶子节点
}

#[derive(Debug)]
struct BPlusTree<K, V> {
    root: NodeRef<K, V>,
}

impl<K: Ord + Clone, V: Clone> BPlusTree<K, V> {
    pub fn new() -> Self {
        let root = Rc::new(RefCell::new(Node::Leaf(LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        })));
        Self { root }
    }

    pub fn insert(&mut self, key: K, value: V) {
        match &mut *self.root.borrow_mut() {
            Node::Leaf(leaf) => {
                let pos = leaf.keys.binary_search(&key).unwrap_or_else(|x| x);
                if pos < leaf.keys.len() && leaf.keys[pos] == key {
                    leaf.values[pos] = value;
                } else {
                    leaf.keys.insert(pos, key);
                    leaf.values.insert(pos, value);
                }
            }
            _ => unimplemented!("需要处理内部节点")
        }
    }

    pub fn search(&self, key: &K) -> Option<V> {
        match &*self.root.borrow() {
            Node::Leaf(leaf) => {
                if let Ok(pos) = leaf.keys.binary_search(key) {
                    Some(leaf.values[pos].clone())
                } else {
                    None
                }
            }
            _ => unimplemented!("需要处理内部节点")
        }
    }

    pub fn delete(&mut self, key: &K) -> bool {
        match &mut *self.root.borrow_mut() {
            Node::Leaf(leaf) => {
                if let Ok(pos) = leaf.keys.binary_search(key) {
                    leaf.keys.remove(pos);
                    leaf.values.remove(pos);
                    true
                } else {
                    false
                }
            }
            _ => unimplemented!("需要处理内部节点")
        }
    }
}

fn main() {
    let mut tree = BPlusTree::<i32, String>::new();
    tree.insert(10, "A".to_string());
    tree.insert(20, "B".to_string());
    tree.insert(30, "C".to_string());

    println!("Search 20: {:?}", tree.search(&20));
    println!("Search 40: {:?}", tree.search(&40));

    tree.delete(&20);
    println!("After delete 20: {:?}", tree.search(&20));

    println!("{:?}", tree);
}

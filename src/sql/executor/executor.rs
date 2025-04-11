use crate::sql::engine::Transaction;
use crate::sql::executor::delete::Delete;
use crate::sql::executor::insert::Insert;
use crate::sql::executor::query::{Order, Scan};
use crate::sql::executor::schema::CreateTable;
use crate::sql::executor::update::Update;
use crate::sql::plan::node::Node;
use crate::sql::types::Row;
use crate::utils::custom_error::LegendDBResult;

// 抽象执行器定义
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self<>>, txn: &mut T) -> LegendDBResult<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable {schema } => CreateTable::new(schema),
            Node::Insert {table_name, columns, values} => Insert::new(table_name, columns, values),
            Node::Scan {table_name, filter} => Scan::new(table_name, filter),
            Node::Update {table_name, source, columns } => Update::new(table_name, Self::build(*source), columns),
            Node::Delete {table_name, source} => Delete::new(table_name, Self::build(*source)),
            Node::CreateDatabase {database_name} => todo!("暂未支持"),
            Node::DropDatabase {database_name} => todo!("暂未支持"),
            Node::DropTable {table_name} => todo!("暂未支持"),
            Node::OrderBy {source, order_by} => Order::new(Self::build(*source), order_by),
        }
    }
}

#[allow(unused)]
// 查询结果集
#[derive(Debug)]
pub enum ResultSet {
    CreateTable {
        table_name: String
    },
    Insert {
        count: usize
    },
    Scan {
        columns: Vec<String>,
        rows: Vec<Row>
    },
    Update {
        count: usize
    },
    Delete {
        count: usize
    },
    Order {
        columns: Vec<String>,
        rows: Vec<Row>
    },
}
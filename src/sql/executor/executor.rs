use crate::sql::engine::engine::Transaction;
use crate::sql::executor::databases::{CreateDataBase, DropDataBase};
use crate::sql::executor::delete::Delete;
use crate::sql::executor::insert::Insert;
use crate::sql::executor::query::{Limit, Offset, Order, Projection, Scan};
use crate::sql::executor::schema::{CreateTable, DropTable};
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
            Node::CreateDatabase {database_name} => CreateDataBase::new(database_name),
            Node::DropDatabase {database_name} => DropDataBase::new(database_name),
            Node::DropTable {table_name} => DropTable::new(table_name),
            Node::OrderBy {source, order_by} => Order::new(Self::build(*source), order_by),
            Node::Limit {source, limit} => Limit::new(Self::build(*source), limit),
            Node::Offset {source, offset} => Offset::new(Self::build(*source), offset),
            Node::Projection {source, columns} => Projection::new(Self::build(*source), columns),
        }
    }
}

#[allow(unused)]
// 查询结果集
#[derive(Debug)]
pub enum ResultSet {
    CreateDatabase {
        database_name: String
    },
    DropDatabase {
        database_name: String
    },
    CreateTable {
        table_name: String
    },
    DropTable {
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
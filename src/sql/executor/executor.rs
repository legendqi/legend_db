use crate::sql::engine::engine::Transaction;
use crate::sql::executor::databases::{CreateDataBaseExecutor, DropDataBaseExecutor};
use crate::sql::executor::delete::DeleteExecutor;
use crate::sql::executor::insert::InsertExecutor;
use crate::sql::executor::join::NestLoopJoinExecutor;
use crate::sql::executor::query::{LimitExecutor, OffsetExecutor, OrderExecutor, ProjectionExecutor, ScanExecutor};
use crate::sql::executor::schema::{CreateTableExecutor, DropTableExecutor};
use crate::sql::executor::update::UpdateExecutor;
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
            Node::CreateTable {schema } => CreateTableExecutor::new(schema),
            Node::Insert {table_name, columns, values} => InsertExecutor::new(table_name, columns, values),
            Node::Scan {table_name, filter} => ScanExecutor::new(table_name, filter),
            Node::Update {table_name, source, columns } => UpdateExecutor::new(table_name, Self::build(*source), columns),
            Node::Delete {table_name, source} => DeleteExecutor::new(table_name, Self::build(*source)),
            Node::CreateDatabase {database_name} => CreateDataBaseExecutor::new(database_name),
            Node::DropDatabase {database_name} => DropDataBaseExecutor::new(database_name),
            Node::DropTable {table_name} => DropTableExecutor::new(table_name),
            Node::OrderBy {source, order_by} => OrderExecutor::new(Self::build(*source), order_by),
            Node::Limit {source, limit} => LimitExecutor::new(Self::build(*source), limit),
            Node::Offset {source, offset} => OffsetExecutor::new(Self::build(*source), offset),
            Node::Projection {source, columns} => ProjectionExecutor::new(Self::build(*source), columns),
            Node::NestedLoopJoin {left, right, predicate, outer} => NestLoopJoinExecutor::new(Self::build(*left), Self::build(*right), predicate, outer),
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
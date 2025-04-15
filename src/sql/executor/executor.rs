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
use crate::custom_error::LegendDBResult;

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

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),
            ResultSet::DropTable { table_name } => format!("DROP TABLE {}", table_name),
            ResultSet::Insert { count } => format!("INSERT {} rows", count),
            ResultSet::Scan { columns, rows } => {
                let rows_len = rows.len();

                // 找到每一列最大的长度
                let mut max_len = columns.iter().map(|c| c.len()).collect::<Vec<_>>();
                for one_row in rows {
                    for (i, v) in one_row.iter().enumerate() {
                        if v.to_string().len() > max_len[i] {
                            max_len[i] = v.to_string().len();
                        }
                    }
                }

                // 展示列
                let columns = columns
                    .iter()
                    .zip(max_len.iter())
                    .map(|(col, &len)| format!("{:width$}", col, width = len))
                    .collect::<Vec<_>>()
                    .join(" |");

                // 展示分隔符
                let sep = max_len
                    .iter()
                    .map(|v| format!("{}", "-".repeat(*v + 1)))
                    .collect::<Vec<_>>()
                    .join("+");

                // 展示列数据
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .zip(max_len.iter())
                            .map(|(v, &len)| format!("{:width$}", v.to_string(), width = len))
                            .collect::<Vec<_>>()
                            .join(" |")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("{}\n{}\n{}\n({} rows)", columns, sep, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),
            ResultSet::Delete { count } => format!("DELETE {} rows", count),
            // ResultSet::Begin { version } => format!("TRANSACTION {} BEGIN", version),
            // ResultSet::Commit { version } => format!("TRANSACTION {} COMMIT", version),
            // ResultSet::Rollback { version } => format!("TRANSACTION {} ROLLBACK", version),
            // ResultSet::Explain { plan } => plan.to_string(),
            _ => {"".to_string()}
        }
    }
}
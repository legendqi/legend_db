use crate::sql::parser::ast::Statement;
use crate::sql::plan::{Node, Plan};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Planner
    }
    pub fn build(&self, stmt: Statement) -> Plan {
        Plan(self.build_statement(stmt))
    }

    pub fn build_statement(&self, stmt: Statement) -> Node {
        match stmt {
            Statement::CreateTable { name, columns } => {
                Node::CreateTable {
                    schema
                }
            },
            Statement::Insert { table_name, columns, values } => {
                Node::Insert {
                    table_name,
                    columns: columns.unwrap(),
                    values
                }
            },
        }
    }
}
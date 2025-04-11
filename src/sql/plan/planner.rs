use crate::sql::parser::ast::{Statement};
use crate::sql::plan::node::{Node, Plan};
use crate::sql::schema::{Column, Table};
use crate::sql::types::Value;

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
                    schema: Table {
                        name,
                        columns: columns.into_iter().map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.is_primary_key);
                            let default = match c.default {
                                Some(v) => Some(Value::from_expression(v)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };
                            Column {
                                name: c.name,
                                data_type: c.data_type,
                                nullable,
                                default_value: default,
                                is_primary_key: c.is_primary_key,
                            }
                        }).collect(),
                    }
                    }
            },
            Statement::Insert { table_name, columns, values } => {
                Node::Insert {
                    table_name,
                    columns: columns.unwrap_or_default(),
                    values
                }
            },
            Statement::Select {table_name, order_by } => {
                let mut scan_node = Node::Scan {
                    table_name,
                    filter: None,
                };
                if order_by.len() > 0 {
                    scan_node = Node::OrderBy {
                        source: Box::new(scan_node),
                        order_by,
                    }
                };
                scan_node
            }
            Statement::Delete { table_name, where_clause } => {
                Node::Delete {
                    table_name: table_name.clone(),
                    source: Box::new(Node::Scan {
                        table_name,
                        filter: where_clause,
                    }),
                }
            },
            Statement::Update { table_name, columns, where_clause } => {
                Node::Update {
                    table_name: table_name.clone(),
                    source: Box::new(Node::Scan {
                        table_name,
                        filter: where_clause,
                    }),
                    columns
                }
            },
            Statement::DropTable { table_name } => {
                Node::DropTable {
                    table_name,
                }
            },
            Statement::CreateDatabase { database_name} => {
                Node::CreateDatabase {
                    database_name,
                }
            },
            Statement::DropDatabase { database_name } => {
                Node::DropDatabase {
                    database_name,
                }
            },
            _ => panic!("Unsupported statement"),
        }
    }
}
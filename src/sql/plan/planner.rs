use crate::sql::parser::ast::{Expression, FromItem, JoinType, Statement};
use crate::sql::plan::node::{Node, Plan};
use crate::sql::schema::{Column, Table};
use crate::sql::types::Value;
use crate::custom_error::{LegendDBError, LegendDBResult};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Planner
    }
    pub fn build(&self, stmt: Statement) -> LegendDBResult<Plan> {
        Ok(Plan(self.build_statement(stmt)?))
    }

    pub fn build_statement(&self, stmt: Statement) -> LegendDBResult<Node> {
        Ok(
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
                Statement::Select {columns, from, where_clause, group_by, having, order_by, limit, offset } => {
                    let mut scan_node = self.build_from_item(from, &where_clause)?;
                    // aggregate, group by
                    let mut has_agg = false;
                    if !columns.is_empty() {
                        for (expr, _) in columns.iter() {
                            // 如果是Function类型就说明给你是agg
                            if let Expression::Function(_, _) = expr {
                                has_agg = true;
                                break;
                            }
                        }
                        if has_agg {
                            // 构造一个聚合节点
                            scan_node = Node::Aggregate {
                                source: Box::new(scan_node),
                                expr: columns.clone(),
                                group_by,
                            };
                        }
                    }
                    // having
                    if let Some(having) = having {
                        scan_node = Node::Filter {
                            source: Box::new(scan_node),
                            predicate: having,
                        }
                    };
                    // 排序 order by
                    if order_by.len() > 0 {
                        scan_node = Node::OrderBy {
                            source: Box::new(scan_node),
                            order_by,
                        }
                    };
                    // Offset 要在Limit 之前解析
                    if let Some(offset) = offset {
                        scan_node = Node::Offset {
                            source: Box::new(scan_node),
                            offset: match Value::from_expression(offset) {
                                Value::Integer(offset) => offset as usize,
                                _ => return Err(LegendDBError::Internal("Offset must be an integer".to_string())),
                            },
                        }
                    };
                    // Limit 要在Offset 之后解析
                    if let Some(limit) = limit {
                        scan_node = Node::Limit {
                            source: Box::new(scan_node),
                            limit: match Value::from_expression(limit) {
                                Value::Integer(limit) => limit as usize,
                                _ => return Err(LegendDBError::Internal("Limit must be an integer".to_string())),
                            },
                        }
                    };
                    
                    // Projection
                    if !columns.is_empty() && !has_agg {
                        scan_node = Node::Projection {
                            source: Box::new(scan_node),
                            columns
                        }
                    }
                    scan_node
                }
                // 删除数据
                Statement::Delete { table_name, where_clause } => {
                    Node::Delete {
                        table_name: table_name.clone(),
                        source: Box::new(Node::Scan {
                            table_name,
                            filter: where_clause,
                        }),
                    }
                },
                // 更新数据
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
                // 删除表
                Statement::DropTable { table_name } => {
                    Node::DropTable {
                        table_name,
                    }
                },
                // 创建数据库
                Statement::CreateDatabase { database_name} => {
                    Node::CreateDatabase {
                        database_name,
                    }
                },
                // 删除数据库
                Statement::DropDatabase { database_name } => {
                    Node::DropDatabase {
                        database_name,
                    }
                },
                // 切换数据库
                Statement::UseDatabase { database_name} => {
                    Node::UseDatabase {
                        database_name,
                    }
                }
            }
        )
    }
    
    pub fn build_from_item(&self, from_item: FromItem, expression: &Option<Vec<Expression>>) -> LegendDBResult<Node> {
        Ok(match from_item { 
            FromItem::Table { name, alias: _ } => {
                Node::Scan {
                    table_name: name,
                    filter: expression.clone(),
                }
            },
            FromItem::Join { left, right, join_type, predicate} => {
                let (left, right) = match join_type { 
                    JoinType::Right => (right, left),
                    _ => (left, right),
                };
                let outer = match join_type { 
                    JoinType::Inner | JoinType::Cross => false,
                    _ => true,
                };
                Node::NestedLoopJoin {
                    left: Box::new(self.build_from_item(*left, expression)?),
                    right: Box::new(self.build_from_item(*right, expression)?),
                    predicate,
                    outer,
                }
            }
        })
    }
}
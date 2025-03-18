mod planner;

use crate::sql::parser::ast::{Expression, Statement};
use crate::sql::plan::planner::Planner;
use crate::sql::schema::Table;
use crate::sql::types::Value;

#[derive(Debug, PartialEq)]
pub enum Node {
    CreateTable {
        schema: Table
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>
    },

    Scan {
        table_name: String,
    },

    Delete {
        table_name: String,
    },
    Update {
        table_name: String,
        set: Vec<(String, Expression)>,
        where_clause: Option<Expression>,
    },
    Drop {
        table_name: String,
    },
}

//执行计划定义，底层是不同类型的节点
#[derive(Debug, PartialEq)]
pub struct Plan(pub Node);


impl Plan {
    pub fn build(stmt: Statement) -> Self {
        Planner::new().build(stmt)
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use crate::{
        sql::{
            parser::{
                ast::{self, Expression},
                Parser,
            },
            plan::{Node, Plan},
        },
    };
    use crate::utils::custom_error::LegendDBResult;

    #[test]
    fn test_plan_create_table() -> LegendDBResult<()> {
        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null,
            c varchar null,
            d bool default true
        );
        ";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1);

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2);
        assert_eq!(p1, p2);

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> LegendDBResult<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1);
        assert_eq!(
            p1,
            Plan(Node::Insert {
                table_name: "tbl1".to_string(),
                columns: vec![],
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Integer(2)),
                    Expression::Consts(ast::Consts::Integer(3)),
                    Expression::Consts(ast::Consts::String("a".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]],
            })
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2);
        assert_eq!(
            p2,
            Plan(Node::Insert {
                table_name: "tbl2".to_string(),
                columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(3)),
                        Expression::Consts(ast::Consts::String("a".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(4)),
                        Expression::Consts(ast::Consts::String("b".to_string())),
                        Expression::Consts(ast::Consts::Boolean(false)),
                    ],
                ],
            })
        );

        Ok(())
    }

    #[test]
    fn test_plan_select() -> LegendDBResult<()> {
        let sql = "select * from tbl1;";
        let stmt = Parser::new(sql).parse()?;
        let p = Plan::build(stmt);
        assert_eq!(
            p,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
            })
        );

        Ok(())
    }
}
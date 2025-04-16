use std::collections::BTreeMap;
use std::iter::Peekable;
use crate::sql::parser::ast::{Column, Consts, Expression, FromItem, JoinType, Operation, OrderDirection, Statement};
use crate::sql::parser::ast::Statement::Select;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::sql::types::DataType;
use crate::custom_error::{LegendDBError, LegendDBResult};

pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>
}


impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable()
        }
    }

    // 解析，获取到抽象语法树
    pub fn parse(&mut self) -> LegendDBResult<Statement> {
        let stmt = self.parse_statement()?;
        // 期望sql语句结束存在分号
        self.next_expect(Token::Semicolon)?;
        //分号之后不能存在其他Token
        if self.custom_peek()?.is_some() {
            return Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> LegendDBResult<Statement> {
        // 查看第一个token类型
        match self.custom_peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword((Keyword::Use))) => self.parse_use(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(token) => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token))),
            None => Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())),
        }
    }
    
    // 解析delete
    fn parse_delete(&mut self) -> LegendDBResult<Statement> {
        self.next_expect(Token::Keyword(Keyword::Delete))?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        let table_name = self.next_ident()?;
        let where_clause = self.parse_where_clause()?;
        Ok(Statement::Delete {
            table_name,
            where_clause,
        })
    }

    // 解析use
    fn parse_use(&mut self) -> LegendDBResult<Statement> {
        self.next_expect(Token::Keyword(Keyword::Use))?;
        let database_name = self.next_ident()?;
        Ok(Statement::UseDatabase {
            database_name,
        })
    }

    // 解析update
    fn parse_update(&mut self) -> LegendDBResult<Statement> {
        // 解析update
        self.next_expect(Token::Keyword(Keyword::Update))?; // update
        // 解析表名
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;
        let mut columns = BTreeMap::new();
        loop {
            let column_name = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let expr = self.parse_expression()?;
            // 判断是否重复
            if columns.contains_key(&column_name) {
                return Err(LegendDBError::Parser(format!("[Parser] Duplicate column {} for update", column_name)));
            }
            columns.insert(column_name, expr);
            // 如果没有逗号则跳出循环
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        let where_clause = self.parse_where_clause()?;
        Ok(Statement::Update {
            table_name,
            columns,
            where_clause,
        })
    }

    // 解析select语句，暂时只支持select * from
    fn parse_select(&mut self) -> LegendDBResult<Statement> {
        // 解析select
        Ok(Select {
            columns: self.parse_select_columns()?,
            from: self.parse_from()?,
            order_by: self.parse_order_by()?,
            limit: {
                if self.next_if_token(Token::Keyword(Keyword::Limit)).is_some() {
                    Some(self.parse_expression()?)
                } else {
                    None
                }
            },
            offset: {
                if self.next_if_token(Token::Keyword(Keyword::Offset)).is_some() {
                    Some(self.parse_expression()?)
                } else {
                    None
                }
            },
        })
    }

    // 解析insert into
    fn parse_insert(&mut self) -> LegendDBResult<Statement> {
        // 解析insert
        self.next_expect(Token::Keyword(Keyword::Insert))?; // insert
        self.next_expect(Token::Keyword(Keyword::Into))?; // into
        // 解析表名
        let table_name = self.next_ident()?;
        // 是否是给指定的列进行insert
        let cols = if self.next_if_token(Token::LeftParen).is_some() {
            let mut columns = vec![];
            loop {
                columns.push(self.next_ident()?.to_string());
                match self.custom_next()? {
                    Token::RightParen => break,
                    Token::Comma => {}
                    token => return Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
                }
            }
            Some(columns)
        } else {
            None
        };
        // 解析values
        self.next_expect(Token::Keyword(Keyword::Values))?;
        //insert into table(a,b,c) values (1,2,3) (4,5,6)
        let mut values = vec![];
        loop {
            self.next_expect(Token::LeftParen)?;
            let mut exprs = vec![];
            loop {
                exprs.push(self.parse_expression()?);
                match self.custom_next()? {
                    Token::RightParen => break,
                    Token::Comma => {}
                    token => return Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
                }
            }
            values.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(Statement::Insert {
            table_name,
            columns: cols,
            values,
        })
    }

    // 解析DDL类型
    fn parse_ddl(&mut self) -> LegendDBResult<Statement> {
        match self.custom_next()? {
            // create 之后为table
            Token::Keyword(Keyword::Create) => match self.custom_next()? {
                Token::Keyword(Keyword::Table) => {
                    self.parse_create_table()
                },
                Token::Keyword(Keyword::Database) => {
                    self.parse_create_database()
                },
                token => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
            },
            // Token::Keyword(Keyword::Drop) => match self.custom_next()? {
            //     Token::Keyword(Keyword::Table) => self.parse_drop_table(),
            //     Token::Keyword(Keyword::Database) => self.parse_drop_database(),
            //     token => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
            // },
            token => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
        }

    }

    /// 解析create table
    fn parse_create_table(&mut self) -> LegendDBResult<Statement> {
        // 期望是一个table的名字
        let table_name = self.next_ident()?;
        // 表名之后是一个括号，里面是字段
        self.next_expect(Token::LeftParen)?;

        // 解析列信息
        let mut columns = vec![];
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果后面没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        self.next_expect(Token::RightParen)?;
        Ok(Statement::CreateTable {
            name: table_name,
            columns,
        })

    }

    fn parse_ddl_column(&mut self) -> LegendDBResult<Column> {
        let mut column = Column {
            name: self.next_ident()?,
            data_type: match self.custom_next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Boolean) | Token::Keyword(Keyword::Bool) => DataType::Boolean,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Varchar) | Token::Keyword(Keyword::Text) => DataType::String,
                token => return Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token))),
            },
            nullable: None,
            default: None,
            is_primary_key: false,
            auto_increment: false,
            unique: false,
        };
        // 解析列的默认值，以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.is_primary_key = true;
                },
                k => return Err(LegendDBError::Parser(format!("[Parser] Unexpected keyword {:?}", k))),
            }
        }
        Ok(column)
    }

    // 解析字段
    // fn parse_field(&mut self) -> LegendDBResult<String> {
    //     let field_name = self.next_ident()?;
    //     self.next_expect(Token::Dot)?;
    //     let table_name = self.next_ident()?;
    //     Ok(Field {
    //         field_name,
    //         table_name,
    //     })
    // }

    // 解析表达式
    fn parse_expression(&mut self) -> LegendDBResult<Expression> {
        Ok(match self.custom_next()? {
            Token::Identifier(ident) => {
                // 解析函数
                if self.next_if_token(Token::LeftParen).is_some() {
                    // 取出列名
                    let col_name = self.next_ident()?;
                    if self.next_if_token(Token::RightParen).is_none() {
                        return Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", self.custom_next()?)));
                    }
                    Expression::Function(ident.clone(), col_name)
                    // 解析函数
                } else {
                    // 解析列名
                    Expression::Field(ident)
                }
            },
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // 整数
                    Consts::Integer(n.parse()?).into()
                } else {
                    // 浮点数
                    Consts::Float(n.parse()?).into()
                }
            }
            Token::String(s) => Consts::String(s).into(),
            Token::Keyword(Keyword::True) => Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => Consts::Null.into(),
            t => {
                return Err(LegendDBError::Parser(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }
    
    // 解析where子句
    fn parse_where_clause(&mut self) -> LegendDBResult<Option<BTreeMap<String, Expression>>> {
        if self.next_if_token(Token::Keyword(Keyword::Where)).is_none() {
            return Ok(None);
        }
        let mut where_clause = BTreeMap::new();
        loop {
            let column_name = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let expr = self.parse_expression()?;
            // 判断是否重复
            if where_clause.contains_key(&column_name) {
                return Err(LegendDBError::Parser(format!("[Parser] Duplicate column {} for update", column_name)));
            }
            where_clause.insert(column_name, expr);
            // // 如果没有and则跳出循环
            if self.next_if_token(Token::Keyword(Keyword::And)).is_none() && self.next_if_token(Token::Keyword(Keyword::Or)).is_none() {
                break;
            }
        }
        Ok(Some(where_clause))
    }
    
    // 解析order by排序
    fn parse_order_by(&mut self) -> LegendDBResult<Vec<(String, OrderDirection)>> {
        if self.next_if_token(Token::Keyword(Keyword::Order)).is_none() {
            return Ok(vec![]);
        }
        self.next_expect(Token::Keyword(Keyword::By))?;
        let mut order_conditions: Vec<(String, OrderDirection)> = Vec::new();
        loop {
            let column_name = self.next_ident()?;
            // let order_keyword = match self.next_if(|x| matches!(x, Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc))) {
            //     Some(Token::Keyword(Keyword::Asc)) => {OrderDirection::Asc}
            //     Some(Token::Keyword(Keyword::Desc)) => {OrderDirection::Desc}
            //     _ => {OrderDirection::Asc}
            // };
            let order = match self.next_if_keyword() {
                Some(Token::Keyword(Keyword::Asc)) => OrderDirection::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            order_conditions.push((column_name, order));
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(order_conditions)
    }
    
    // 解析查询的列信息
    fn parse_select_columns(&mut self) -> LegendDBResult<Vec<(Expression, Option<String>)>> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        let mut columns = vec![];
        if self.next_if_token(Token::Star).is_some() {
            return Ok(columns);
        }
        loop {
            let expr = self.parse_expression()?;
            // 是否有别名
            let alias = match self.next_if_token(Token::Keyword(Keyword::As)) {
                Some(_) => {
                    Some(self.next_ident()?)
                },
                None => None
            };
            columns.push((expr, alias));
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(columns)
    }

    // 解析from相关，包含join
    fn parse_from(&mut self) -> LegendDBResult<FromItem> {
        self.next_expect(Token::Keyword(Keyword::From))?;
        // 第一个表名
        let mut first_item = self.parse_from_table()?;
        // 是否有join
        while let Some(join_type) = self.parser_from_join()?{
            let left = Box::new(first_item.clone());
            let right = Box::new(self.parse_from_table()?);
            // 解析 join类型
            let predicate = match join_type {
                JoinType::Cross => None,
                _ => {
                    self.next_expect(Token::Keyword(Keyword::On))?;
                    let left_expr = self.parse_expression()?;
                    self.next_expect(Token::Equal)?;
                    let right_expr = self.parse_expression()?;
                    // 右连接，左表为右表， 右连接，右表为左表
                    let (left_expr, right_expr) = match join_type { 
                        JoinType::Right => {
                            (right_expr, left_expr)
                        }
                        _ => {
                            (left_expr, right_expr)
                        }
                    };
                    // 构建条件 左表中的一列等于右表中的一列
                    let cond = Operation::Equal(Box::new(left_expr), Box::new(right_expr));
                    Some(Expression::Operation(cond))
                }
            };
            first_item = FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            }
        }
        Ok(first_item)
    }

    fn parse_from_table(&mut self) -> LegendDBResult<FromItem> {
        // 判断是否有别名
        let alias = match self.next_if_token(Token::Keyword(Keyword::As)) {
            Some(_) => {
                Some(self.next_ident()?)
            },
            None => None
        };
        // 解析字段
        Ok(FromItem::Table {name: self.next_ident()?, alias})
    }

    fn parser_from_join(&mut self) -> LegendDBResult<Option<JoinType>> {
        // 是否是cross join
        if self.next_if_token(Token::Keyword(Keyword::Cross)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            return Ok(Some(JoinType::Cross)); // 返回cross join
        } else if self.next_if_token(Token::Keyword(Keyword::Join)).is_some() {
            return Ok(Some(JoinType::Inner)); // 返回inner join
        } else if self.next_if_token(Token::Keyword(Keyword::Left)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            return Ok(Some(JoinType::Left)); // 返回left join
        } else if self.next_if_token(Token::Keyword(Keyword::Right)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            return Ok(Some(JoinType::Right)); // 返回right join
        }
        Ok(None)
    }

    // 解析创建数据库
    fn parse_create_database(&mut self) -> LegendDBResult<Statement> {
        Ok(Statement::CreateDatabase {
            database_name: self.next_ident()?,
        })
    }

    // fn parse_create_database(&mut self) -> LegendDBResult<Statement> {
    //     match self.next_ident()? {
    //         Some(Token::Identifier(ident)) => {
    //             // 创建数据库
    //             match ident.as_str() {
    //                 "database" => {
    //                     Ok(Statement::CreateDatabase {
    //                         name: ident,
    //                     })
    //                 },
    //                 "table" => {
    //                     Ok(Statement::CreateTable {
    //                         name: ident,
    //                         columns: vec![],
    //                     })
    //                 }
    //             }
    //         },
    //         None => Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())),
    //         _ => Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()))
    //     }
    // }

    fn custom_peek(&mut self) -> LegendDBResult<Option<Token>> {
        //transpose 方法作用是将 Option<Result<T, E>> 转换为 Result<Option<T>, E>
        // Option<Result<T, E>> 转换为 Result<Option<T>, E>
        // 如果 Option 是 Some(Ok(value))，则返回 Ok(Some(value))。
        // 如果 Option 是 Some(Err(error))，则返回 Err(error)。
        // 如果 Option 是 None，则返回 Ok(None)。
        // Result<Option<T>, E>> 转换为 Option<Result<T, E>>
        // 如果 Result 是 Ok(Some(value))，则返回 Some(Ok(value))。
        // 如果 Result 是 Ok(None)，则返回 None。
        // 如果 Result 是 Err(error)，则返回 Some(Err(error))。
        self.lexer.peek().cloned().transpose()
    }

    fn custom_next(&mut self) -> LegendDBResult<Token> {
        self.lexer.next().unwrap_or_else(|| Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())))
    }

    fn next_ident(&mut self) -> LegendDBResult<String> {
        match self.custom_next()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
        }
    }

    fn next_expect(&mut self, expected: Token) -> LegendDBResult<()> {
        match self.custom_next()? {
            token if token == expected => Ok(()),
            token => Err(LegendDBError::Parser(format!("[Parser] Expected token: {:?}, got {}", expected, token)))
        }
    }
    // 如果满足条件，则跳转到下一个Token，否则返回None
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.custom_peek().unwrap_or(None).filter(|t|predicate(t))?;
        self.custom_next().ok()
    }

    // 如果下一个Token是关键字，则跳转
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    // 如果下一个 Token 是关键字，则跳转
    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::parser::Consts;
use std::collections::BTreeMap;
    use crate::{sql::parser::ast};
    use crate::sql::parser::ast::{Statement};
    use crate::custom_error::LegendDBResult;
    use super::Parser;

    #[test]
    fn test_parser_create_table() -> LegendDBResult<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let stmt3 = Parser::new(sql3).parse();
        assert!(stmt3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> LegendDBResult<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> LegendDBResult<()> {
        let sql = "select a as col1, b as col2, c from tbl1 limit 10 offset 20;";
        let stmt = Parser::new(sql).parse()?;
        println!("{:?}", stmt);
        // assert_eq!(
        //     stmt,
        //     Statement::Select  {
        //         columns: vec![],
        //         table_name: "tbl1".to_string(),
        //         order_by: vec![],
        //         limit: Some(Expression::Consts(Consts::Integer(10))),
        //         offset: Some(Expression::Consts(Consts::Integer(20))),
        //     }
        // );
        Ok(())
    }

    #[test]
    fn test_parser_update() -> LegendDBResult<()> {
        let sql = "update tbl1 set a = 1, b = 2 where c = 3 and d = 4;";
        let stmt = Parser::new(sql).parse()?;
        println!("{:?}", stmt);
        let mut columns = BTreeMap::new();
        columns.insert("a".to_string(), Consts::Integer(1).into());
        columns.insert("b".to_string(), Consts::Integer(2).into());
        let mut where_clause = BTreeMap::new();
        where_clause.insert("c".to_string(), Consts::Integer(3).into());
        where_clause.insert("d".to_string(), Consts::Integer(4).into());
        assert_eq!(
            stmt,
            Statement::Update {
                table_name: "tbl1".to_string(),
                columns,
                where_clause: Some(where_clause),
            }
        );
        Ok(())
    }
}